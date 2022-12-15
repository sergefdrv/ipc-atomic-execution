use anyhow::{bail, Result};
use cid::{multihash::Code, Cid};
use fvm_ipld_blockstore::Blockstore;
use fvm_ipld_encoding::{Cbor, CborStore};
use fvm_ipld_hamt::BytesKey;
use fvm_primitives::{TCid, THamt};
use fvm_shared::{econ::TokenAmount, ActorID, bigint::Zero, HAMT_BIT_WIDTH};
use integer_encoding::VarInt;
use ipc_atomic_execution::AtomicInputState;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct State {
    pub name: String,
    pub symbol: String,
    pub total: TokenAmount,
    pub balances: TCid<THamt<ActorID, AtomicInputState<TokenAmount>>>,
}
impl Cbor for State {}

// type BalanceMap<'bs, BS> = Map<'bs, BS, TokenAmount>;

impl State {
    pub fn new(
        bs: &mut impl Blockstore,
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
                bail!("duplicate balance for {}", a)
            }
        }
        Ok(Self {
            name,
            symbol,
            total,
            balances: TCid::from(balance_map.flush()?),
        })
    }

    pub fn load(bs: &impl Blockstore, cid: &Cid) -> anyhow::Result<Option<Self>> {
        Ok(bs.get_cbor(cid)?)
    }

    pub fn flush(&self, bs: &mut impl Blockstore) -> anyhow::Result<Cid> {
        bs.put_cbor(self, Code::Blake2b256)
    }

    pub fn total_supply(&self) -> TokenAmount {
        self.total.clone()
    }

    pub fn balance(&self, bs: &impl Blockstore, id: ActorID) -> Result<Option<TokenAmount>> {
        let k = BytesKey::from(id.encode_var_vec());
        Ok(self.balances.load(bs)?.get(&k)?.map(|a| a.get().clone()))
    }

    pub fn transfer(
        &mut self,
        bs: &impl Blockstore,
        from: ActorID,
        to: ActorID,
        amount: TokenAmount,
    ) -> anyhow::Result<(TokenAmount, TokenAmount)> {
        Ok(self.balances.modify(bs, |m| {
            let from_key = BytesKey::from(from.encode_var_vec());
            let mut from_state = m.get(&from_key)?.cloned().unwrap_or_default();

            let to_key = BytesKey::from(to.encode_var_vec());
            let mut to_state = m.get(&to_key)?.cloned().unwrap_or_default();

            if *from_state < amount {
                bail!("insufficient balance");
            }

            from_state.modify(|b| Ok(*b -= &amount))?;
            to_state.modify(|b| Ok(*b += &amount))?;

            Ok((from_state.get().clone(), to_state.get().clone()))
        })?)
    }
}
