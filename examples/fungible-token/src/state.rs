use std::collections::HashMap;

use fvm_ipld_blockstore::Blockstore;
use fvm_ipld_encoding::{Cbor, RawBytes};
use fvm_ipld_hamt::BytesKey;
use fvm_primitives::{TCid, THamt};
use fvm_shared::{address::Address, bigint::Zero, econ::TokenAmount, ActorID, HAMT_BIT_WIDTH};
use integer_encoding::VarInt;
use ipc_atomic_execution::{AtomicExecID, AtomicExecRegistry, AtomicInputID, AtomicInputState};
use ipc_gateway::IPCAddress;
use serde::{Deserialize, Serialize};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};

pub type AccountState = AtomicInputState<TokenAmount>;

#[derive(Serialize, Deserialize)]
pub struct State {
    ipc_gateway: Address,
    ipc_address: IPCAddress,
    atomic_exec_coordinator: IPCAddress,
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
        ipc_gateway: Address,
        ipc_address: IPCAddress,
        atomic_exec_coordinator: IPCAddress,
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
            ipc_gateway,
            ipc_address,
            atomic_exec_coordinator,
            name,
            symbol,
            total,
            balances: TCid::from(balance_map.flush()?),
            atomic_registry: AtomicExecRegistry::new(bs)?,
        })
    }

    pub fn ipc_gateway(&self) -> Address {
        self.ipc_gateway
    }

    pub fn ipc_address(&self) -> &IPCAddress {
        &self.ipc_address
    }

    pub fn atomic_exec_coordinator(&self) -> &IPCAddress {
        &self.atomic_exec_coordinator
    }

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

    pub fn cancel_atomic_transfer(
        &mut self,
        bs: &impl Blockstore,
        input_id: AtomicInputID,
    ) -> anyhow::Result<()> {
        let atomic_registry = &mut self.atomic_registry;
        let input = atomic_registry
            .atomic_input(bs, &input_id)?
            .ok_or_else(|| anyhow::anyhow!("unexpected own input ID"))?;
        let AtomicTransfer { from, .. } = input.deserialize()?;
        let from_key = Self::account_key(from);
        let mut balances = self.balances.load(bs)?;
        let mut from_state = balances.get(&from_key)?.cloned().unwrap_or_default();
        atomic_registry.cancel_atomic_exec(bs, input_id, std::iter::once(&mut from_state))?;
        balances.set(from_key, from_state)?;
        self.balances.flush(balances)?;
        Ok(())
    }

    pub fn prep_atomic_transfer(
        &mut self,
        bs: &impl Blockstore,
        input_ids: &HashMap<IPCAddress, AtomicInputID>,
    ) -> anyhow::Result<AtomicExecID> {
        let own_input_id = input_ids
            .get(self.ipc_address())
            .ok_or_else(|| anyhow::anyhow!("missing own input ID"))?;
        let atomic_registry = &mut self.atomic_registry;
        let input = atomic_registry
            .atomic_input(bs, &own_input_id)?
            .ok_or_else(|| anyhow::anyhow!("unexpected own input ID"))?;
        let AtomicTransfer { from, .. } = input.deserialize()?;
        let from_key = Self::account_key(from);
        let mut balances = self.balances.load(bs)?;
        let mut from_state = balances.get(&from_key)?.cloned().unwrap_or_default();
        let exec_id = atomic_registry.prepare_atomic_exec(
            bs,
            &own_input_id,
            input_ids,
            std::iter::once(&mut from_state),
            input,
        )?;

        balances.set(from_key, from_state)?;
        self.balances.flush(balances)?;

        Ok(exec_id)
    }

    pub fn commit_atomic_transfer(
        &mut self,
        bs: &impl Blockstore,
        exec_id: AtomicExecID,
    ) -> anyhow::Result<()> {
        let atomic_registry = &mut self.atomic_registry;
        let output = atomic_registry
            .atomic_output(bs, &exec_id)?
            .ok_or_else(|| anyhow::anyhow!("unexpected exec ID"))?;
        let AtomicTransfer { from, to, amount } = output.deserialize()?;
        let from_key = Self::account_key(from);
        let mut balances = self.balances.load(bs)?;
        let mut from_state = balances.get(&from_key)?.cloned().unwrap_or_default();
        atomic_registry.commit_atomic_exec(bs, exec_id, std::iter::once(&mut from_state))?;
        balances.set(from_key, from_state)?;
        self.balances.flush(balances)?;
        self.transfer(bs, from, to, amount)?;
        Ok(())
    }

    pub fn rollback_atomic_transfer(
        &mut self,
        bs: &impl Blockstore,
        exec_id: AtomicExecID,
    ) -> anyhow::Result<()> {
        let atomic_registry = &mut self.atomic_registry;
        let output = atomic_registry
            .atomic_output(bs, &exec_id)?
            .ok_or_else(|| anyhow::anyhow!("unexpected exec ID"))?;
        let AtomicTransfer { from, .. } = output.deserialize()?;
        let from_key = Self::account_key(from);
        let mut balances = self.balances.load(bs)?;
        let mut from_state = balances.get(&from_key)?.cloned().unwrap_or_default();
        atomic_registry.rollback_atomic_exec(bs, exec_id, std::iter::once(&mut from_state))?;
        balances.set(from_key, from_state)?;
        self.balances.flush(balances)?;
        Ok(())
    }

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
