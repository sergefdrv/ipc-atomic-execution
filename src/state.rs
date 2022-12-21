// Copyright: ConsensusLab
//
use fvm_ipld_blockstore::Blockstore;
use fvm_ipld_encoding::Cbor;
use fvm_ipld_hamt::BytesKey;
use fvm_shared::address::Address;
use fvm_shared::MethodNum;
use ipc_gateway::IPCAddress;
use primitives::{TCid, THamt};
use serde::{Deserialize, Serialize};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::collections::{HashMap, HashSet};

use crate::{AtomicExecID, ConstructorParams};

#[derive(Serialize, Deserialize)]
pub struct State {
    pub ipc_gateway_address: Address,
    pub registry: RegistryCid, // (exec_id, actors) -> pre-commitments
}
impl Cbor for State {}

// TODO: Use hash/CID as the key?
type RegistryCid = TCid<THamt<RegistryKey, RegistryEntry>>;
type RegistryEntry = HashMap<IPCAddress, MethodNum>;

#[derive(Clone, PartialEq, Serialize_tuple, Deserialize_tuple)]
pub struct RegistryKey {
    exec_id: AtomicExecID,
    actors: HashSet<IPCAddress>,
}
impl Cbor for RegistryKey {}

impl State {
    pub fn new<BS: Blockstore>(store: &BS, params: ConstructorParams) -> anyhow::Result<State> {
        Ok(State {
            registry: TCid::new_hamt(store)?,
            ipc_gateway_address: params.ipc_gateway_address,
        })
    }

    /// Modifies the atomic execution entry associated with the atomic
    /// execution ID and the actors.
    pub fn modify_atomic_exec<BS: Blockstore, R>(
        &mut self,
        store: &BS,
        exec_id: AtomicExecID,
        actors: HashSet<IPCAddress>,
        f: impl FnOnce(&mut HashMap<IPCAddress, MethodNum>) -> anyhow::Result<R>,
    ) -> anyhow::Result<R> {
        self.registry.modify(store, |registry| {
            let k = BytesKey::from(RegistryKey { exec_id, actors }.marshal_cbor()?);
            let mut entry = registry
                .get(&k)?
                .map_or_else(HashMap::new, |e| e.to_owned());
            let res = f(&mut entry)?;
            registry.set(k, entry)?;
            Ok(res)
        })
    }

    /// Removes the atomic execution entry associated with the atomic
    /// execution ID and the actors.
    pub fn rm_atomic_exec<BS: Blockstore>(
        &mut self,
        store: &BS,
        exec_id: AtomicExecID,
        actors: HashSet<IPCAddress>,
    ) -> anyhow::Result<()> {
        let k = BytesKey::from(RegistryKey { exec_id, actors }.marshal_cbor()?);
        self.registry.update(store, |registry| {
            registry.delete(&k)?;
            Ok(())
        })?;
        Ok(())
    }
}
