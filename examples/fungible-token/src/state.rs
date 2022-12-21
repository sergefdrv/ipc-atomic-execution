use std::collections::HashMap;

use fvm_ipld_blockstore::Blockstore;
use fvm_ipld_encoding::{Cbor, RawBytes};
use fvm_ipld_hamt::BytesKey;
use fvm_primitives::{TCid, THamt};
use fvm_shared::{bigint::Zero, econ::TokenAmount, ActorID, HAMT_BIT_WIDTH};
use integer_encoding::VarInt;
use ipc_atomic_execution::{AtomicExecID, AtomicExecRegistry, AtomicInputID, AtomicInputState};
use ipc_gateway::IPCAddress;
use serde::{Deserialize, Serialize};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};

pub type AccountState = AtomicInputState<TokenAmount>;

#[derive(Serialize, Deserialize)]
pub struct State {
    name: String,
    symbol: String,
    total: TokenAmount,
    balances: TCid<THamt<ActorID, AccountState>>,
    atomic_registry: AtomicExecRegistry,
}
impl Cbor for State {}

impl State {
    pub fn new(
        bs: &impl Blockstore,
        name: String,
        symbol: String,
        balances: impl IntoIterator<Item = (ActorID, TokenAmount)>,
    ) -> anyhow::Result<Self> {
        let mut total = TokenAmount::zero();
        let mut balance_map = fvm_actors_runtime::make_empty_map(bs, HAMT_BIT_WIDTH);
        for (a, b) in balances {
            let k = BytesKey::from(a.encode_var_vec());
            if balance_map.set_if_absent(k, b.clone())? {
                total += b;
            } else {
                anyhow::bail!("duplicate balance for {}", a)
            }
        }
        Ok(Self {
            name,
            symbol,
            total,
            balances: TCid::from(balance_map.flush()?),
            atomic_registry: AtomicExecRegistry::new(bs)?,
        })
    }

    // pub fn load(bs: &impl Blockstore, cid: &Cid) -> anyhow::Result<Option<Self>> {
    //     bs.get_cbor(cid)
    // }

    // pub fn flush(&self, bs: &mut impl Blockstore) -> anyhow::Result<Cid> {
    //     bs.put_cbor(self, Code::Blake2b256)
    // }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn total_supply(&self) -> TokenAmount {
        self.total.clone()
    }

    pub fn balance(&self, bs: &impl Blockstore, id: ActorID) -> anyhow::Result<TokenAmount> {
        Ok(self
            .balances
            .load(bs)?
            .get(&Self::account_key(id))?
            .map(|a| a.get().clone())
            .unwrap_or_default())
    }

    pub fn transfer(
        &mut self,
        bs: &impl Blockstore,
        from: ActorID,
        to: ActorID,
        amount: TokenAmount,
    ) -> anyhow::Result<(TokenAmount, TokenAmount)> {
        self.balances.modify(bs, |m| {
            let from_key = Self::account_key(from);
            let mut from_state = m.get(&from_key)?.cloned().unwrap_or_default();

            let to_key = Self::account_key(to);
            let mut to_state = m.get(&to_key)?.cloned().unwrap_or_default();

            if *from_state < amount {
                anyhow::bail!("insufficient balance");
            }

            from_state.modify(|b| Ok(*b -= &amount))?;
            to_state.modify(|b| Ok(*b += &amount))?;

            let from_balance = from_state.get().clone();
            let to_balance = to_state.get().clone();

            m.set(from_key, from_state)?;
            m.set(to_key, to_state)?;

            Ok((from_balance, to_balance))
        })
    }

    pub fn init_atomic_transfer(
        &mut self,
        bs: &impl Blockstore,
        from: ActorID,
        to: ActorID,
        amount: TokenAmount,
    ) -> anyhow::Result<AtomicInputID> {
        let from_key = Self::account_key(from);
        let mut balances = self.balances.load(bs)?;
        let mut from_state = balances.get(&from_key)?.cloned().unwrap_or_default();

        if *from_state < amount {
            anyhow::bail!("insufficient balance");
        }

        let input_id = self.atomic_registry.init_atomic_exec(
            bs,
            std::iter::once(&mut from_state),
            RawBytes::serialize(AtomicTransfer { from, to, amount })?,
            true,
        )?;

        balances.set(from_key, from_state)?;

        self.balances.flush(balances)?;

        Ok(input_id)
    }

    pub fn prep_atomic_transfer(
        &mut self,
        bs: &impl Blockstore,
        own_input_id: AtomicInputID,
        input_ids: &HashMap<IPCAddress, AtomicInputID>,
    ) -> anyhow::Result<AtomicExecID> {
        let atomic_registry = &mut self.atomic_registry;
        let mut balances = self.balances.load(bs)?;
        let mut from_key = None;
        let mut from_state = None;
        let exec_id = atomic_registry.prepare_atomic_exec(
            bs,
            own_input_id,
            input_ids,
            |input| {
                let AtomicTransfer { from, .. } = RawBytes::deserialize(&input)?;
                from_key = Some(Self::account_key(from));
                from_state = Some(
                    balances
                        .get(from_key.as_ref().unwrap())?
                        .cloned()
                        .unwrap_or_default(),
                );

                Ok((input, Box::new(from_state.as_mut().into_iter())))
            },
            |input| Ok(input),
        )?;

        balances.set(from_key.unwrap(), from_state.unwrap())?;
        self.balances.flush(balances)?;

        Ok(exec_id)
    }

    // fn account_state(&self, bs: &impl Blockstore, id: ActorID) -> anyhow::Result<AccountState> {
    //     Ok(self
    //         .balances
    //         .load(bs)?
    //         .get(&Self::account_key(id))?
    //         .cloned()
    //         .unwrap_or_default())
    // }

    // fn set_account_state(
    //     &mut self,
    //     bs: &impl Blockstore,
    //     id: ActorID,
    //     s: AccountState,
    // ) -> anyhow::Result<Option<AccountState>> {
    //     self.balances
    //         .modify(bs, |m| Ok(m.set(Self::account_key(id), s)?))
    // }

    fn account_key(id: ActorID) -> BytesKey {
        BytesKey::from(id.encode_var_vec())
    }
}

#[derive(Serialize_tuple, Deserialize_tuple)]
struct AtomicTransfer {
    from: ActorID,
    to: ActorID,
    amount: TokenAmount,
}
