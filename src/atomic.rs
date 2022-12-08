use cid::multihash::{Blake2b256, MultihashDigest};
use cid::multihash::{Code, Hasher};
use fvm_ipld_hamt::BytesKey;
use ipc_gateway::IPCAddress;
use std::cell::Cell;
use std::collections::HashMap;
use std::ops::Deref;

use cid::Cid;
use fvm_ipld_blockstore::Blockstore;
use fvm_ipld_encoding::tuple::Deserialize_tuple;
use fvm_ipld_encoding::{Cbor, CborStore, RawBytes, DAG_CBOR};
use primitives::{TCid, THamt};
use serde::Deserialize;
use serde::{self, de::DeserializeOwned, Serialize};
use serde_tuple::Serialize_tuple;

/// State that supports locking, as well as computing its CID.
pub trait LockableState: Cbor {
    /// Locks the state so that it cannot be changed until unlocked.
    fn lock(&mut self) -> anyhow::Result<()>;

    /// Unlocks the state and allows it to be modified.
    fn unlock(&mut self) -> anyhow::Result<()>;

    /// Checks if the state is locked.
    fn is_locked(&self) -> bool;

    /// Returns current state CID.
    fn cid(&self) -> Cid {
        cid_from_cbor(self)
    }
}

/// Computes the CID of a CBOR object.
fn cid_from_cbor(obj: &impl Cbor) -> Cid {
    Cid::new_v1(
        DAG_CBOR,
        Code::Blake2b256.digest(&obj.marshal_cbor().unwrap()),
    )
}

/// Lockable piece of actor state that can be used as an input for
/// atomic execution.
///
/// It can be either incorporated into other data structure, or
/// referred to by its CID. In the latter case, it is user's
/// responsibility to flush to and load from the blockstore.
#[derive(Debug, Serialize_tuple, Deserialize_tuple)]
pub struct AtomicInputState<T>
where
    T: Serialize + DeserializeOwned,
{
    // Cached CID value representing the current content.
    #[serde(skip)]
    cid: Cell<Option<Cid>>,

    // Flag indicating if the state is locked.
    locked: bool,

    // Arbitrary piece of state.
    state: T,
}
impl<T: Serialize + DeserializeOwned> Cbor for AtomicInputState<T> {}

impl<T: Serialize + DeserializeOwned> AtomicInputState<T> {
    /// Converts some state into a lockable piece of state.
    pub fn new(state: T) -> Self {
        Self {
            cid: Cell::new(None),
            locked: false,
            state,
        }
    }

    /// Attempts to load the content from the blockstore.
    pub fn load(cid: &Cid, bs: &impl Blockstore) -> anyhow::Result<Option<Self>> {
        let res = bs.get_cbor::<Self>(cid)?;
        if let Some(s) = res.as_ref() {
            s.cid.set(Some(*cid)); // cache known CID
        }
        Ok(res)
    }

    /// Flushes the content to the blockstore.
    pub fn flush(&self, bs: &impl Blockstore) -> anyhow::Result<Cid> {
        let cid = bs.put_cbor(&self, Code::Blake2b256)?;
        self.cid.set(Some(cid)); // cache computed CID
        Ok(cid)
    }

    /// Attempts to get a mutable reference to the inner content;
    /// fails if the state is locked.
    pub fn get_mut(&mut self) -> anyhow::Result<&mut T> {
        match self.locked {
            false => {
                self.cid.set(None); // invalidate cached CID
                Ok(&mut self.state)
            }
            true => Err(anyhow::anyhow!("cannot modify locked state")),
        }
    }

    /// Attempts to modify the inner content in the supplied closure,
    /// as well as to produce some result value; fails if locked.
    pub fn modify<F, R>(&mut self, f: F) -> anyhow::Result<R>
    where
        F: FnOnce(&mut T) -> anyhow::Result<R>,
    {
        let s = self.get_mut()?;
        let r = f(s)?;
        Ok(r)
    }
}

impl<T: Serialize + DeserializeOwned> Deref for AtomicInputState<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.state
    }
}

impl<T: Serialize + DeserializeOwned> LockableState for AtomicInputState<T> {
    fn lock(&mut self) -> anyhow::Result<()> {
        match self.locked {
            false => {
                self.cid.set(None); // invalidate cached CID
                self.locked = true;
                Ok(())
            }
            true => Err(anyhow::anyhow!("state already locked")),
        }
    }

    fn unlock(&mut self) -> anyhow::Result<()> {
        match self.locked {
            true => {
                self.cid.set(None); // invalidate cached CID
                self.locked = false;
                Ok(())
            }
            false => Err(anyhow::anyhow!("state not locked")),
        }
    }

    fn is_locked(&self) -> bool {
        self.locked
    }

    fn cid(&self) -> Cid {
        match self.cid.get() {
            Some(cid) => cid,
            None => {
                let cid = cid_from_cbor(self);
                self.cid.set(Some(cid)); // cache computed CID
                cid
            }
        }
    }
}

impl<T: Default + Serialize + DeserializeOwned> Default for AtomicInputState<T> {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

/// Arbitrary data associated with an atomic execution input.
pub type AtomicInput = RawBytes;

/// Concise identifier of an atomic execution input.
pub type AtomicInputID = RawBytes;

/// Arbitrary data associated with an atomic execution ID.
pub type AtomicOutput = RawBytes;

/// Concise identifier of an atomic execution instance.
pub type AtomicExecID = RawBytes;

type AtomicExecNonce = u64;

/// Internal state associated with an atomic execution input.
#[derive(Debug, PartialEq, Serialize_tuple, Deserialize_tuple)]
struct AtomicInputEntry {
    unlocked_state_cids: Vec<Cid>,
    input: AtomicInput,
}
impl Cbor for AtomicInputEntry {}

/// Internal state associated with an atomic execution ID.
#[derive(Debug, PartialEq, Serialize_tuple, Deserialize_tuple)]
struct AtomicOutputEntry {
    output: AtomicOutput,
}

/// Registry of atomic execution instances.
///
/// Each atomic execution actor should maintain a single instance of
/// it as a part of its state. It can be either incorporated into a
/// bigger data structure, or referred to by its CID. In the latter
/// case, it is user's responsibility to flush to and load from the
/// blockstore.
#[derive(Debug, Serialize, Deserialize)]
pub struct AtomicExecRegistry {
    nonce: AtomicExecNonce,
    input_ids: TCid<THamt<AtomicInputID, AtomicInputEntry>>,
    exec_ids: TCid<THamt<AtomicExecID, AtomicOutputEntry>>,
}
impl Cbor for AtomicExecRegistry {}

impl AtomicExecRegistry {
    /// Constructs a new instance of the atomic execution registry.
    ///
    /// It flushes its internals to the supplied blockstore. However,
    /// the registry itself is not flushed to the blockstore.
    pub fn new(bs: &impl Blockstore) -> anyhow::Result<AtomicExecRegistry> {
        Ok(Self {
            nonce: 0,
            input_ids: TCid::new_hamt(bs)?,
            exec_ids: TCid::new_hamt(bs)?,
        })
    }

    /// Loads the atomic execution registry from the supplied
    /// blockstore by its CID.
    pub fn load(cid: &Cid, bs: &impl Blockstore) -> anyhow::Result<Option<AtomicExecRegistry>> {
        bs.get_cbor(cid)
    }

    /// Flushes the atomic execution registry to the supplied
    /// blockstore and return its CID.
    pub fn flush(&self, bs: &impl Blockstore) -> anyhow::Result<Cid> {
        let cid = bs.put_cbor(&self, Code::Blake2b256)?;
        Ok(cid)
    }

    /// Initializes a new instance of the atomic execution protocol.
    ///
    /// It returns a unique identifier of the atomic execution input.
    ///
    /// The supplied iterable collection `state` represents pieces of
    /// actor's state that are involved in the atomic execution.
    ///
    /// `input` is any data to associate with the returned input ID.
    ///
    /// If `lock` is set to `true` then the method automatically locks
    /// the supplied state; otherwise it just captures the state CIDs
    /// to check against when calling
    /// [`prepare_atomic_exec`](Self::prepare_atomic_exec). In that
    /// case, the caller is responsible for flushing the supplied
    /// lockable state to the blockstore.
    pub fn init_atomic_exec<'a, S>(
        &mut self,
        bs: &impl Blockstore,
        state: impl IntoIterator<Item = &'a mut S>,
        input: AtomicInput,
        lock: bool,
    ) -> anyhow::Result<AtomicInputID>
    where
        S: LockableState + 'a,
    {
        // Optionally lock the state and compute its CIDs
        let unlocked_state_cids = state.into_iter().try_fold(Vec::new(), |mut v, s| {
            if lock {
                s.lock()?;
            } else {
                v.push(s.cid());
            }
            anyhow::Ok(v)
        })?;

        // Generate and register a new input ID
        let input_id = self.new_input_id(&unlocked_state_cids, &input);
        self.input_ids.modify(bs, |m| {
            let k = BytesKey::from(input_id.bytes());
            let v = m.set(
                k,
                AtomicInputEntry {
                    unlocked_state_cids,
                    input,
                },
            )?;
            assert!(v.is_some(), "input ID collision");
            Ok(())
        })?;

        Ok(input_id)
    }

    /// Consumes and discards the supplied atomic execution input ID.
    ///
    /// This cancels the associated initiated instance of the atomic
    /// execution protocol.
    ///
    /// The supplied closure `input_fn` receives the data associated
    /// with the input ID and must return an iterator over the
    /// lockable state matching the one previously supplied to the
    /// corresponding invocation of
    /// [`init_atomic_exec`](Self::init_atomic_exec).
    ///
    /// Any locked piece of the state is automatically unlocked by the
    /// method.
    pub fn cancel_atomic_exec<'a, S>(
        &mut self,
        bs: &impl Blockstore,
        input_id: AtomicInputID,
        input_fn: impl FnOnce(AtomicInput) -> Box<dyn Iterator<Item = &'a mut S>>,
    ) -> anyhow::Result<()>
    where
        S: LockableState + 'a,
    {
        // Consume own input ID and retrieve the associated data
        let AtomicInputEntry { input, .. } = self.input_ids.modify(bs, |m| {
            let k = BytesKey::from(input_id.bytes());
            let (_, v) = m
                .delete(&k)?
                .ok_or_else(|| anyhow::anyhow!("unexpected input ID"))?;
            Ok(v)
        })?;

        // Get the state and ensure it's unlocked
        let state_iter = input_fn(input);
        state_iter.for_each(|s| {
            if s.is_locked() {
                s.unlock().unwrap();
            }
        });

        Ok(())
    }

    /// Consumes the supplied own atomic execution input ID and
    /// produces an atomic execution identifier.
    ///
    /// It returns a unique identifier of the atomic execution to be
    /// submitted to the coordinator actor in a cross-net message.
    ///
    /// Every executing actor should agree on the supplied input IDs
    /// `input_ids`, which should include the supplied `own_input_id`.
    ///
    /// The supplied closure `input_fn` receives the data associated
    /// with `own_input_id`, interprets the data, and returns it
    /// together with an iterator over the lockable state. The
    /// lockable state must match the one previously supplied to the
    /// corresponding invocation of
    /// [`init_atomic_exec`](Self::init_atomic_exec). Any unlocked
    /// piece of the state is automatically locked by the method.
    ///
    /// The supplied closure `output_fn` receives the data
    /// interpretation returned by `input_fn` and returns any data to
    /// associate with the returned atomic execution ID.
    pub fn prepare_atomic_exec<'a, S, I>(
        &mut self,
        bs: &impl Blockstore,
        own_input_id: AtomicInputID,
        input_ids: &HashMap<IPCAddress, AtomicInputID>,
        input_fn: impl FnOnce(AtomicInput) -> (I, Box<dyn Iterator<Item = &'a mut S>>),
        output_fn: impl FnOnce(I) -> anyhow::Result<AtomicOutput>,
    ) -> anyhow::Result<AtomicExecID>
    where
        S: 'a + LockableState,
    {
        // Consume own input ID and retrieve the associated data
        let AtomicInputEntry {
            unlocked_state_cids,
            input,
        } = self.input_ids.modify(bs, |m| {
            let k = BytesKey::from(own_input_id.bytes());
            let (_, v) = m
                .delete(&k)?
                .ok_or_else(|| anyhow::anyhow!("unexpected own input ID"))?;
            Ok(v)
        })?;

        // Get the input and the state; check that the state has not
        // changed and ensure it is locked
        let (input, state_iter) = input_fn(input);
        let unlocked_state_cid_iter = state_iter.filter(|s| !s.is_locked()).map(|s| {
            let cid = s.cid();
            s.lock().unwrap();
            cid
        });
        if !unlocked_state_cid_iter.eq(unlocked_state_cids) {
            anyhow::bail!("state CID mismatch");
        }

        // Compute the atomic execution ID; produce and store the
        // output
        let exec_id = Self::compute_exec_id(input_ids);
        self.exec_ids.modify(bs, |m| {
            let k = BytesKey::from(exec_id.bytes());
            let output = output_fn(input)?;
            let v = m.set(k, AtomicOutputEntry { output })?;
            assert!(v.is_none(), "exec ID collision");
            Ok(())
        })?;

        Ok(exec_id)
    }

    /// Consumes the supplied atomic execution ID and commits the
    /// result of an atomic execution.
    ///
    /// The supplied closure `output_fn` receives the data associated
    /// with `exec_id`, interprets the data, and returns it together
    /// with an iterator over the lockable state. The lockable state
    /// should match the one previously supplied to the corresponding
    /// invocation of [`init_atomic_exec`](Self::init_atomic_exec).
    /// The state is automatically unlocked by the method.
    ///
    /// The supplied closure `apply_fn` receives the data
    /// interpretation returned by `output_fn` in order to merge it
    /// into the actor state.
    pub fn commit_atomic_exec<'a, S, O, R>(
        &mut self,
        bs: &impl Blockstore,
        exec_id: AtomicExecID,
        output_fn: impl FnOnce(AtomicOutput) -> (O, Box<dyn Iterator<Item = &'a mut S>>),
        apply_fn: impl FnOnce(O) -> anyhow::Result<R>,
    ) -> anyhow::Result<R>
    where
        S: 'a + LockableState,
    {
        // Consume the atomic exec ID and retrieve the associated data
        let AtomicOutputEntry { output } = self.exec_ids.modify(bs, |m| {
            let k = BytesKey::from(exec_id.bytes());
            let (_, v) = m
                .delete(&k)?
                .ok_or_else(|| anyhow::anyhow!("unexpected atomic exec ID"))?;
            Ok(v)
        })?;

        // Get the output and the state; unlock the state
        let (output, state_iter) = output_fn(output);
        state_iter.for_each(|s| s.unlock().unwrap());

        // Apply the output and return the result
        let res = apply_fn(output)?;
        Ok(res)
    }

    /// Consumes the supplied atomic execution ID and rolls the atomic
    /// execution back.
    ///
    /// The supplied closure `output_fn` functions same as in
    /// [`commit_atomic_exec`](Self::commit_atomic_exec).
    ///
    /// The supplied closure `rollback_fn` receives the data
    /// interpretation returned by `output_fn` and can be used to
    /// revert any additional changes made to the actor state.
    pub fn rollback_atomic_exec<'a, S, O>(
        &mut self,
        bs: &impl Blockstore,
        exec_id: AtomicExecID,
        output_fn: impl FnOnce(AtomicOutput) -> (O, Box<dyn Iterator<Item = &'a mut S>>),
        rollback_fn: impl FnOnce(O),
    ) -> anyhow::Result<()>
    where
        S: 'a + LockableState,
    {
        // Consume the atomic exec Id and retrieve let associated data
        let AtomicOutputEntry { output } = self.exec_ids.modify(bs, |m| {
            let k = BytesKey::from(exec_id.bytes());
            let (_, v) = m
                .delete(&k)?
                .ok_or_else(|| anyhow::anyhow!("unexpected atomic exec ID"))?;
            Ok(v)
        })?;

        // Get and unlock the state
        let (output, state_iter) = output_fn(output);
        state_iter.for_each(|s| s.unlock().unwrap());

        // Rollback using the output
        rollback_fn(output);

        Ok(())
    }

    fn new_input_id<'a>(
        &mut self,
        unlocked_state_cids: impl IntoIterator<Item = &'a Cid>,
        input: &RawBytes,
    ) -> AtomicInputID {
        let nonce = self.nonce;
        self.nonce += 1; // ensure uniqueness of the input ID

        let mut h = Blake2b256::default();
        h.update(&RawBytes::serialize(nonce).unwrap());
        for s in unlocked_state_cids {
            h.update(&RawBytes::serialize(s).unwrap());
        }
        h.update(input);
        Vec::from(h.finalize()).into()
    }

    fn compute_exec_id(input_ids: &HashMap<IPCAddress, AtomicInputID>) -> AtomicExecID {
        let mut h = Blake2b256::default();
        h.update(&RawBytes::serialize(input_ids).unwrap());
        Vec::from(h.finalize()).into()
    }
}
