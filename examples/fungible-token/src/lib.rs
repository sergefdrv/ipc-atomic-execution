use std::collections::HashMap;

use frc42_dispatch::{match_method, method_hash};
use fvm_actors_runtime::runtime::{ActorCode, Runtime};
use fvm_actors_runtime::{actor_error, cbor, ActorDowncast, ActorError, INIT_ACTOR_ADDR};
use fvm_ipld_blockstore::Blockstore;
use fvm_ipld_encoding::RawBytes;
use fvm_shared::address::Address;
use fvm_shared::econ::TokenAmount;
use fvm_shared::error::ExitCode;
use fvm_shared::MethodNum;
use ipc_atomic_execution::Method::{PreCommit, Revoke};
use ipc_atomic_execution::{AtomicExecID, AtomicInputID, PreCommitParams, RevokeParams};
use ipc_gateway::{ApplyMsgParams, CrossMsg, IPCAddress, StorableMsg};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use state::State;

mod state;

fvm_actors_runtime::wasm_trampoline!(Actor);

struct Actor;

#[derive(Clone, Serialize_tuple, Deserialize_tuple)]
pub struct ConstructorParams {
    pub ipc_gateway: Address,
    pub ipc_address: IPCAddress,
    pub atomic_exec_coordinator: IPCAddress,
    pub name: String,
    pub symbol: String,
    pub balances: HashMap<Address, TokenAmount>,
}

/// Instruction to transfer tokens to another address
#[derive(Serialize_tuple, Deserialize_tuple, Clone, Debug)]
pub struct TransferParams {
    pub to: Address,
    /// A non-negative amount to transfer
    pub amount: TokenAmount,
    /// Arbitrary data to pass on via the receiver hook
    pub operator_data: RawBytes,
}

/// Return value after a successful transfer
#[derive(Serialize_tuple, Deserialize_tuple, Clone, Debug)]
pub struct TransferReturn {
    /// The new balance of the `from` address
    pub from_balance: TokenAmount,
    /// The new balance of the `to` address
    pub to_balance: TokenAmount,
    /// (Optional) data returned from receiver hook
    pub recipient_data: RawBytes,
}

#[derive(Serialize_tuple, Deserialize_tuple, Clone, Debug)]
pub struct PrepareAtomicParams {
    pub input_ids: HashMap<IPCAddress, AtomicInputID>,
}

#[derive(Serialize_tuple, Deserialize_tuple, Clone, Debug)]
pub struct AbortAtomicParams {
    pub input_ids: HashMap<IPCAddress, AtomicInputID>,
    pub exec_id: AtomicExecID,
}

impl Actor {
    fn constructor<BS, RT>(rt: &mut RT, params: ConstructorParams) -> Result<(), ActorError>
    where
        BS: Blockstore + Clone,
        RT: Runtime<BS>,
    {
        rt.validate_immediate_caller_is(std::iter::once(&*INIT_ACTOR_ADDR))?;

        let ConstructorParams {
            ipc_gateway,
            ipc_address,
            atomic_exec_coordinator,
            name,
            symbol,
            balances,
        } = params;
        let balances = balances.into_iter().try_fold(HashMap::new(), |mut m, (a, b)| {
            let id = rt.resolve_address(&a).ok_or_else(|| actor_error!(illegal_argument; "cannot resolve address in initial balance table"))?;
            m.insert(id.id().unwrap(), b);
            Ok::<_, ActorError>(m)
        })?;
        let st = State::new(
            rt.store(),
            ipc_gateway,
            ipc_address,
            atomic_exec_coordinator,
            name,
            symbol,
            balances,
        )
        .map_err(|e| {
            e.downcast_default(ExitCode::USR_ILLEGAL_STATE, "failed to create actor state")
        })?;
        rt.create(&st)?;
        Ok(())
    }

    fn name<BS, RT>(rt: &mut RT) -> Result<String, ActorError>
    where
        BS: Blockstore + Clone,
        RT: Runtime<BS>,
    {
        let st: State = rt.state()?;
        Ok(st.name().to_string())
    }

    fn symbol<BS, RT>(rt: &mut RT) -> Result<String, ActorError>
    where
        BS: Blockstore + Clone,
        RT: Runtime<BS>,
    {
        let st: State = rt.state()?;
        Ok(st.symbol().to_string())
    }

    fn total_supply<BS, RT>(rt: &mut RT) -> Result<TokenAmount, ActorError>
    where
        BS: Blockstore + Clone,
        RT: Runtime<BS>,
    {
        let st: State = rt.state()?;
        Ok(st.total_supply())
    }

    fn balance<BS, RT>(rt: &mut RT, addr: Address) -> Result<TokenAmount, ActorError>
    where
        BS: Blockstore + Clone,
        RT: Runtime<BS>,
    {
        let id = rt
            .resolve_address(&addr)
            .ok_or_else(|| actor_error!(illegal_argument; "cannot resolve account address"))?;
        let st: State = rt.state()?;
        let b = st.balance(rt.store(), id.id().unwrap()).map_err(|e| {
            e.downcast_default(
                ExitCode::USR_ILLEGAL_STATE,
                "failed to get balance from store",
            )
        })?;
        Ok(b)
    }

    fn transfer<BS, RT>(rt: &mut RT, params: TransferParams) -> Result<TransferReturn, ActorError>
    where
        BS: Blockstore + Clone,
        RT: Runtime<BS>,
    {
        let TransferParams {
            to,
            amount,
            operator_data: _,
        } = params;

        let from_id = rt.message().caller();
        let to_id = rt.resolve_address(&to).ok_or_else(
            || actor_error!(illegal_argument; "cannot resolve destination account address"),
        )?;

        let (from_balance, to_balance) = rt.transaction(|st: &mut State, rt| {
            st.transfer(
                rt.store(),
                from_id.id().unwrap(),
                to_id.id().unwrap(),
                amount,
            )
            .map_err(|e| e.downcast_default(ExitCode::USR_UNSPECIFIED, "cannot perform transfer"))
        })?;

        Ok(TransferReturn {
            from_balance,
            to_balance,
            recipient_data: RawBytes::default(),
        })
    }

    fn init_atomic_transfer<BS, RT>(
        rt: &mut RT,
        params: TransferParams,
    ) -> Result<AtomicInputID, ActorError>
    where
        BS: Blockstore + Clone,
        RT: Runtime<BS>,
    {
        let TransferParams {
            to,
            amount,
            operator_data: _,
        } = params;

        let from_id = rt.message().caller();
        let to_id = rt.resolve_address(&to).ok_or_else(
            || actor_error!(illegal_argument; "cannot resolve destination account address"),
        )?;

        let input_id = rt.transaction(|st: &mut State, rt| {
            st.init_atomic_transfer(
                rt.store(),
                from_id.id().unwrap(),
                to_id.id().unwrap(),
                amount,
            )
            .map_err(|e| {
                e.downcast_default(ExitCode::USR_UNSPECIFIED, "cannot init atomic transfer")
            })
        })?;
        Ok(input_id)
    }

    fn cancel_atomic_transfer<BS, RT>(
        rt: &mut RT,
        input_id: AtomicInputID,
    ) -> Result<(), ActorError>
    where
        BS: Blockstore + Clone,
        RT: Runtime<BS>,
    {
        rt.transaction(|st: &mut State, rt| {
            st.cancel_atomic_transfer(rt.store(), input_id)
                .map_err(|e| {
                    e.downcast_default(ExitCode::USR_UNSPECIFIED, "cannot cancel atomic transfer")
                })
        })
    }

    fn prapre_atomic_transfer<BS, RT>(
        rt: &mut RT,
        params: PrepareAtomicParams,
    ) -> Result<AtomicExecID, ActorError>
    where
        BS: Blockstore + Clone,
        RT: Runtime<BS>,
    {
        let PrepareAtomicParams { input_ids } = params;

        let st: State = rt.state()?;
        let exec_id = rt.transaction(|st: &mut State, rt| {
            let exec_id = st
                .prep_atomic_transfer(rt.store(), &input_ids)
                .map_err(|e| {
                    e.downcast_default(ExitCode::USR_UNSPECIFIED, "cannot prepare atomic transfer")
                })?;
            Ok(exec_id)
        })?;

        let msg = CrossMsg {
            msg: ipc_gateway::StorableMsg {
                to: st.atomic_exec_coordinator().to_owned(),
                method: PreCommit as MethodNum,
                params: RawBytes::serialize(PreCommitParams {
                    actors: input_ids.keys().cloned().collect(),
                    exec_id: exec_id.clone(),
                    commit: method_hash!("CommitAtomicTransfer"), // requires literal string
                })?,
                ..Default::default()
            },
            wrapped: true,
        };
        rt.send(
            st.ipc_gateway(),
            ipc_gateway::Method::SendCross as MethodNum,
            RawBytes::serialize(msg)?,
            TokenAmount::default(),
        )?;

        Ok(exec_id)
    }

    fn commit_atomic_transfer<BS, RT>(rt: &mut RT, params: ApplyMsgParams) -> Result<(), ActorError>
    where
        BS: Blockstore + Clone,
        RT: Runtime<BS>,
    {
        let st: State = rt.state()?;

        // Check if the cross-message comes from the IPC gateway actor
        rt.validate_immediate_caller_is(std::iter::once(&st.ipc_gateway()))?;

        let ApplyMsgParams {
            cross_msg:
                CrossMsg {
                    msg: StorableMsg { from, params, .. },
                    ..
                },
        } = params;

        let exec_id = cbor::deserialize_params(&params)?;

        if from != *st.atomic_exec_coordinator() {
            actor_error!(forbidden; "unexpected cross-net message origin");
        }

        rt.transaction(|st: &mut State, rt| {
            st.commit_atomic_transfer(rt.store(), exec_id).map_err(|e| {
                e.downcast_default(ExitCode::USR_UNSPECIFIED, "cannot commit atomic transfer")
            })
        })
    }

    fn abort_atomic_transfer<BS, RT>(
        rt: &mut RT,
        params: AbortAtomicParams,
    ) -> Result<(), ActorError>
    where
        BS: Blockstore + Clone,
        RT: Runtime<BS>,
    {
        let AbortAtomicParams { input_ids, exec_id } = params;

        let st: State = rt.state()?;
        let msg = CrossMsg {
            msg: ipc_gateway::StorableMsg {
                to: st.atomic_exec_coordinator().to_owned(),
                method: Revoke as MethodNum,
                params: RawBytes::serialize(RevokeParams {
                    actors: input_ids.keys().cloned().collect(),
                    exec_id,
                    rollback: method_hash!("RollbackAtomicTransfer"),
                })?,
                ..Default::default()
            },
            wrapped: true,
        };
        rt.send(
            st.ipc_gateway(),
            ipc_gateway::Method::SendCross as MethodNum,
            RawBytes::serialize(msg)?,
            TokenAmount::default(),
        )?;
        Ok(())
    }

    fn rollback_atomic_transfer<BS, RT>(
        rt: &mut RT,
        params: ApplyMsgParams,
    ) -> Result<(), ActorError>
    where
        BS: Blockstore + Clone,
        RT: Runtime<BS>,
    {
        let st: State = rt.state()?;

        // Check if the cross-message comes from the IPC gateway actor
        rt.validate_immediate_caller_is(std::iter::once(&st.ipc_gateway()))?;

        let ApplyMsgParams {
            cross_msg:
                CrossMsg {
                    msg: StorableMsg { from, params, .. },
                    ..
                },
        } = params;

        let exec_id = cbor::deserialize_params(&params)?;

        if from != *st.atomic_exec_coordinator() {
            actor_error!(forbidden; "unexpected cross-net message origin");
        }

        rt.transaction(|st: &mut State, rt| {
            st.rollback_atomic_transfer(rt.store(), exec_id)
                .map_err(|e| {
                    e.downcast_default(ExitCode::USR_UNSPECIFIED, "cannot rollback atomic transfer")
                })
        })
    }
}

impl ActorCode for Actor {
    fn invoke_method<BS, RT>(
        rt: &mut RT,
        method: MethodNum,
        params: &RawBytes,
    ) -> Result<RawBytes, ActorError>
    where
        BS: Blockstore + Clone,
        RT: Runtime<BS>,
    {
        match_method!(method, {
            "Constructor" => {
                Self::constructor(rt, cbor::deserialize_params(params)?)?;
                Ok(RawBytes::default())
            },
            "Name" => {
                let res = Self::name(rt)?;
                Ok(RawBytes::serialize(res)?)
            },
            "Symbol" => {
                let res = Self::symbol(rt)?;
                Ok(RawBytes::serialize(res)?)
            },
            "TotalSupply" => {
                let res = Self::total_supply(rt)?;
                Ok(RawBytes::serialize(res)?)
            },
            "Balance" => {
                let res = Self::balance(rt, cbor::deserialize_params(params)?)?;
                Ok(RawBytes::serialize(res)?)
            },
            "Transfer" => {
                let res = Self::transfer(rt, cbor::deserialize_params(params)?)?;
                Ok(RawBytes::serialize(res)?)
            },
            "InitAtomicTransfer" => {
                let res = Self::init_atomic_transfer(rt, cbor::deserialize_params(params)?)?;
                Ok(RawBytes::serialize(res)?)
            },
            "PrepareAtomicTransfer" => {
                let res = Self::prapre_atomic_transfer(rt, cbor::deserialize_params(params)?)?;
                Ok(RawBytes::serialize(res)?)
            },
            "CancelAtomicTransfer" => {
                let res = Self::cancel_atomic_transfer(rt, cbor::deserialize_params(params)?)?;
                Ok(RawBytes::serialize(res)?)
            },
            "CommitAtomicTransfer" => {
                let res = Self::commit_atomic_transfer(rt, cbor::deserialize_params(params)?)?;
                Ok(RawBytes::serialize(res)?)
            },
            "AbortAtomicTransfer" => {
                let res = Self::abort_atomic_transfer(rt, cbor::deserialize_params(params)?)?;
                Ok(RawBytes::serialize(res)?)
            },
            "RollbackAtomicTransfer" => {
                let res = Self::rollback_atomic_transfer(rt, cbor::deserialize_params(params)?)?;
                Ok(RawBytes::serialize(res)?)
            },
            _ => Err(actor_error!(unhandled_message; "Invalid method")),
        })
    }
}
