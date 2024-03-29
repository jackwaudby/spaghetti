use crate::common::isolation_level::IsolationLevel;

// use crate::cpp::{Mutex as RwLock, MutexGuard, SharedMutexGuard};

use parking_lot::Mutex;
use rustc_hash::FxHashSet;
use spin::{RwLock, RwLockReadGuard as SharedMutexGuard, RwLockWriteGuard as MutexGuard};

use std::cell::UnsafeCell;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

unsafe impl<'a> Send for Node {}
unsafe impl<'a> Sync for Node {}

pub type EdgeSet = Mutex<FxHashSet<Edge>>;

#[derive(Clone, Eq, Hash, PartialEq)]
pub enum Edge {
    ReadWrite(usize),
    NotReadWrite(usize),
}

impl Edge {
    pub fn extract_id(&self) -> usize {
        let id = match &self {
            Edge::ReadWrite(id) => id,
            Edge::NotReadWrite(id) => id,
        };

        *id
    }
}

#[derive(Debug)]
pub struct Node {
    isolation_level: Option<IsolationLevel>,
    node_id: UnsafeCell<Option<usize>>,
    incoming: UnsafeCell<Option<EdgeSet>>,
    outgoing: UnsafeCell<Option<EdgeSet>>,
    committed: AtomicBool,
    cascading: AtomicBool,
    aborted: AtomicBool,
    cleaned: AtomicBool,
    checked: AtomicBool,
    terminated: AtomicBool,
    lock: RwLock<u8>,
    abort_through: UnsafeCell<usize>,
}

impl Node {
    pub fn read(&self) -> SharedMutexGuard<u8> {
        self.lock.read()
    }

    pub fn write(&self) -> MutexGuard<u8> {
        self.lock.write()
    }

    pub fn new(
        incoming: EdgeSet,
        outgoing: EdgeSet,
        isolation_level: Option<IsolationLevel>,
    ) -> Self {
        Self {
            isolation_level,
            node_id: UnsafeCell::new(None),
            incoming: UnsafeCell::new(Some(incoming)),
            outgoing: UnsafeCell::new(Some(outgoing)),
            committed: AtomicBool::new(false),
            cascading: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
            cleaned: AtomicBool::new(false),
            checked: AtomicBool::new(false),
            terminated: AtomicBool::new(false),

            lock: RwLock::new(0),
            abort_through: UnsafeCell::new(0),
        }
    }

    pub fn get_isolation_level(&self) -> IsolationLevel {
        self.isolation_level.unwrap()
    }

    /// Returns `true` if an edge from a given node already exists in this node's incoming edge set.
    pub fn incoming_edge_exists(&self, from: &Edge) -> bool {
        let incoming_edges = unsafe { self.incoming.get().as_ref() };

        match incoming_edges {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let guard = edges.lock();
                    let exists = guard.contains(from);
                    drop(guard);
                    exists
                }
                None => panic!("incoming edge set removed"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Returns `true` if the node has at least 1 incoming edge.
    pub fn has_incoming(&self) -> bool {
        let incoming_edges = unsafe { self.incoming.get().as_ref() };

        match incoming_edges {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let guard = edges.lock();
                    let res = guard.is_empty();
                    drop(guard);
                    !res
                }
                None => panic!("incoming edge set removed"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Insert an incoming edge: (from) --> (this)
    pub fn insert_incoming(&self, from: Edge) {
        let incoming_edges = unsafe { self.incoming.get().as_ref() };

        match incoming_edges {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let mut guard = edges.lock();
                    guard.insert(from);
                    drop(guard);
                }
                None => panic!("incoming edge set removed"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Remove an edge from this node's incoming edge set.
    pub fn remove_incoming(&self, from: &Edge) {
        let incoming_edges = unsafe { self.incoming.get().as_ref() };

        match incoming_edges {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let mut guard = edges.lock();
                    assert_eq!(
                        guard.remove(from),
                        true,
                        "Trying to remove: {:?}, Current: {:?}",
                        from,
                        *guard
                    );
                    drop(guard);
                }
                None => panic!("incoming edge set removed"),
            },
            None => panic!("check unsafe"),
        }
    }

    pub fn insert_outgoing(&self, to: Edge) {
        let outgoing_edges = unsafe { self.outgoing.get().as_ref() };

        match outgoing_edges {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let mut guard = edges.lock();
                    guard.insert(to);
                    drop(guard);
                }
                None => panic!("outgoing edge set already cleaned"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Remove incoming edge set from node.
    pub fn take_incoming(&self) -> EdgeSet {
        let incoming = unsafe { self.incoming.get().as_mut() };

        match incoming {
            Some(edge_set) => match edge_set.take() {
                Some(edges) => edges,
                None => panic!("incoming edge set already removed"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Remove outgoing edge set from node.
    pub fn take_outgoing(&self) -> EdgeSet {
        let outgoing = unsafe { self.outgoing.get().as_mut() };

        match outgoing {
            Some(edge_set) => match edge_set.take() {
                Some(edges) => edges,
                None => panic!("outgoing edge set already removed"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Get a clone of the outgoing edge from node.
    pub fn get_outgoing(&self) -> FxHashSet<Edge> {
        match unsafe { self.outgoing.get().as_ref().unwrap().as_ref() } {
            Some(edges) => {
                let guard = edges.lock();
                let out = guard.clone();
                drop(guard);
                out
            }
            None => FxHashSet::default(),
        }
    }

    /// Get a clone of the outgoing edge from node.
    pub fn get_incoming(&self) -> FxHashSet<Edge> {
        match unsafe { self.incoming.get().as_ref().unwrap().as_ref() } {
            Some(edges) => {
                let guard = edges.lock();
                let out = guard.clone();
                drop(guard);
                out
            }
            None => FxHashSet::default(),
        }
    }

    pub fn set_id(&self, node_id: usize) {
        unsafe { *self.node_id.get().as_mut().unwrap() = Some(node_id) };
    }

    pub fn get_id(&self) -> Option<usize> {
        unsafe { self.node_id.get().as_mut().unwrap().clone() }
    }

    pub fn is_aborted(&self) -> bool {
        self.aborted.load(Ordering::Acquire)
    }

    pub fn set_aborted(&self) {
        self.aborted.store(true, Ordering::Release);
    }

    pub fn is_terminated(&self) -> bool {
        self.terminated.load(Ordering::Acquire)
    }

    pub fn set_terminated(&self) {
        self.terminated.store(true, Ordering::Release);
    }

    pub fn is_cascading_abort(&self) -> bool {
        self.cascading.load(Ordering::Acquire)
    }

    pub fn set_cascading_abort(&self) {
        self.cascading.store(true, Ordering::Release);
    }

    pub fn is_committed(&self) -> bool {
        self.committed.load(Ordering::Acquire)
    }

    pub fn set_committed(&self) {
        self.committed.store(true, Ordering::Release);
    }

    pub fn is_checked(&self) -> bool {
        self.checked.load(Ordering::Acquire)
    }

    pub fn set_checked(&self, val: bool) {
        self.checked.store(val, Ordering::Release);
    }

    pub fn is_cleaned(&self) -> bool {
        self.cleaned.load(Ordering::Acquire)
    }

    pub fn set_cleaned(&self) {
        self.cleaned.store(true, Ordering::Release);
    }

    pub fn set_abort_through(&self, id: usize) {
        unsafe { *self.abort_through.get() = id };
    }
    pub fn get_abort_through(&self) -> usize {
        unsafe { *self.abort_through.get() }
    }
}

impl std::fmt::Debug for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        use Edge::*;
        match &*self {
            ReadWrite(txn_id) => write!(f, "{}", format!("[rw: {:x}]", txn_id)),
            NotReadWrite(txn_id) => write!(f, "{}", format!("[ww: {:x}]", txn_id)),
            // WriteRead(txn_id) => write!(f, "{}", format!("[wr: {:x}]", txn_id)),
        }
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[t-id: {}]", format!("{:x}", self.get_id().unwrap()),)
    }
}

// MSGT

unsafe impl<'a> Send for MsgNode {}
unsafe impl<'a> Sync for MsgNode {}

pub type MsgEdgeSet = Mutex<FxHashSet<MsgEdge>>;

#[derive(Clone, Eq, Hash, PartialEq)]
pub enum MsgEdge {
    ReadWrite(usize),
    WriteWrite(usize),
    WriteRead(usize),
}

impl MsgEdge {
    pub fn extract_id(&self) -> usize {
        let id = match &self {
            MsgEdge::ReadWrite(id) => id,
            MsgEdge::WriteRead(id) => id,
            MsgEdge::WriteWrite(id) => id,
        };

        *id
    }
}

#[derive(Debug)]
pub struct MsgNode {
    isolation_level: Option<IsolationLevel>,
    node_id: UnsafeCell<Option<usize>>,
    incoming: UnsafeCell<Option<MsgEdgeSet>>,
    outgoing: UnsafeCell<Option<MsgEdgeSet>>,
    committed: AtomicBool,
    cascading: AtomicBool,
    aborted: AtomicBool,
    cleaned: AtomicBool,
    checked: AtomicBool,
    terminated: AtomicBool,
    commit_phase: AtomicBool,
    lock: RwLock<u8>,
    abort_through: UnsafeCell<usize>,
    at_risk: AtomicBool,
}

impl MsgNode {
    pub fn read(&self) -> SharedMutexGuard<u8> {
        self.lock.read()
    }

    pub fn write(&self) -> MutexGuard<u8> {
        self.lock.write()
    }

    pub fn new(
        incoming: MsgEdgeSet,
        outgoing: MsgEdgeSet,
        isolation_level: Option<IsolationLevel>,
    ) -> Self {
        Self {
            isolation_level,
            node_id: UnsafeCell::new(None),
            incoming: UnsafeCell::new(Some(incoming)),
            outgoing: UnsafeCell::new(Some(outgoing)),
            committed: AtomicBool::new(false),
            cascading: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
            cleaned: AtomicBool::new(false),
            checked: AtomicBool::new(false),
            terminated: AtomicBool::new(false),
            commit_phase: AtomicBool::new(false),
            lock: RwLock::new(0),
            abort_through: UnsafeCell::new(0),
            at_risk: AtomicBool::new(false),
        }
    }

    pub fn get_isolation_level(&self) -> IsolationLevel {
        self.isolation_level.unwrap()
    }

    /// Returns `true` if an edge from a given node already exists in this node's incoming edge set.
    pub fn incoming_edge_exists(&self, from: &MsgEdge) -> bool {
        let incoming_edges = unsafe { self.incoming.get().as_ref() };

        match incoming_edges {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let guard = edges.lock();
                    let exists = guard.contains(from);
                    drop(guard);
                    exists
                }
                None => panic!("incoming edge set removed"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Returns `true` if the node has at least 1 incoming edge.
    pub fn has_incoming(&self) -> bool {
        let incoming_edges = unsafe { self.incoming.get().as_ref() };

        match incoming_edges {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let guard = edges.lock();
                    let res = guard.is_empty();
                    drop(guard);
                    !res
                }
                None => panic!("incoming edge set removed"),
            },
            None => panic!("check unsafe"),
        }
    }

    pub fn has_incoming_weaker(&self) -> bool {
        let incoming_edges = unsafe { self.incoming.get().as_ref() };

        match incoming_edges {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let guard = edges.lock();

                    for edge in &*guard {
                        let from_ref = unsafe { &*(edge.extract_id() as *const MsgNode) };
                        let from_iso = from_ref.get_isolation_level();
                        let committing = from_ref.is_in_commit_phase();
                        if !committing {
                            continue;
                        }
                        match self.get_isolation_level() {
                            IsolationLevel::Serializable => match from_iso {
                                IsolationLevel::Serializable => {}
                                IsolationLevel::ReadCommitted => {
                                    drop(guard);
                                    return true;
                                }
                                IsolationLevel::ReadUncommitted => {
                                    drop(guard);
                                    return true;
                                }
                            },
                            IsolationLevel::ReadCommitted => match from_iso {
                                IsolationLevel::Serializable => {}
                                IsolationLevel::ReadCommitted => {}
                                IsolationLevel::ReadUncommitted => {
                                    drop(guard);
                                    return true;
                                }
                            },
                            IsolationLevel::ReadUncommitted => match from_iso {
                                IsolationLevel::Serializable => {}
                                IsolationLevel::ReadCommitted => {}
                                IsolationLevel::ReadUncommitted => {}
                            },
                        }
                        // }
                    }

                    drop(guard);
                    return false;
                }
                None => panic!("incoming edge set removed"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Insert an incoming edge: (from) --> (this)
    pub fn insert_incoming(&self, from: MsgEdge) {
        let incoming_edges = unsafe { self.incoming.get().as_ref() };

        match incoming_edges {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let mut guard = edges.lock();
                    guard.insert(from);
                    drop(guard);
                }
                None => panic!("incoming edge set removed"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Remove an edge from this node's incoming edge set.
    pub fn remove_incoming(&self, from: &MsgEdge) {
        let incoming_edges = unsafe { self.incoming.get().as_ref() };

        match incoming_edges {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let mut guard = edges.lock();
                    assert_eq!(
                        guard.remove(from),
                        true,
                        "Trying to remove: {:?}, Current: {:?}",
                        from,
                        *guard
                    );
                    drop(guard);
                }
                None => panic!("incoming edge set removed"),
            },
            None => panic!("check unsafe"),
        }
    }

    pub fn insert_outgoing(&self, to: MsgEdge) {
        let outgoing_edges = unsafe { self.outgoing.get().as_ref() };

        match outgoing_edges {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let mut guard = edges.lock();
                    guard.insert(to);
                    drop(guard);
                }
                None => panic!("outgoing edge set already cleaned"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Remove incoming edge set from node.
    pub fn take_incoming(&self) -> MsgEdgeSet {
        let incoming = unsafe { self.incoming.get().as_mut() };

        match incoming {
            Some(edge_set) => match edge_set.take() {
                Some(edges) => edges,
                None => panic!("incoming edge set already removed"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Remove outgoing edge set from node.
    pub fn take_outgoing(&self) -> MsgEdgeSet {
        let outgoing = unsafe { self.outgoing.get().as_mut() };

        match outgoing {
            Some(edge_set) => match edge_set.take() {
                Some(edges) => edges,
                None => panic!("outgoing edge set already removed"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Get a clone of the outgoing edge from node.
    pub fn get_outgoing(&self) -> FxHashSet<MsgEdge> {
        match unsafe { self.outgoing.get().as_ref().unwrap().as_ref() } {
            Some(edges) => {
                let guard = edges.lock();
                let out = guard.clone();
                drop(guard);
                out
            }
            None => FxHashSet::default(),
        }
    }

    /// Get a clone of the outgoing edge from node.
    pub fn get_incoming(&self) -> FxHashSet<MsgEdge> {
        match unsafe { self.incoming.get().as_ref().unwrap().as_ref() } {
            Some(edges) => {
                let guard = edges.lock();
                let out = guard.clone();
                drop(guard);
                out
            }
            None => FxHashSet::default(),
        }
    }

    pub fn set_id(&self, node_id: usize) {
        unsafe { *self.node_id.get().as_mut().unwrap() = Some(node_id) };
    }

    pub fn get_id(&self) -> Option<usize> {
        unsafe { self.node_id.get().as_mut().unwrap().clone() }
    }

    pub fn is_aborted(&self) -> bool {
        self.aborted.load(Ordering::Acquire)
    }

    pub fn set_aborted(&self) {
        self.aborted.store(true, Ordering::Release);
    }

    pub fn is_in_commit_phase(&self) -> bool {
        self.commit_phase.load(Ordering::Acquire)
    }

    pub fn set_commit_phase(&self) {
        self.commit_phase.store(true, Ordering::Release);
    }

    pub fn is_terminated(&self) -> bool {
        self.terminated.load(Ordering::Acquire)
    }

    pub fn set_terminated(&self) {
        self.terminated.store(true, Ordering::Release);
    }

    pub fn is_cascading_abort(&self) -> bool {
        self.cascading.load(Ordering::Acquire)
    }

    pub fn set_cascading_abort(&self) {
        self.cascading.store(true, Ordering::Release);
    }

    pub fn is_at_risk(&self) -> bool {
        self.at_risk.load(Ordering::Acquire)
    }

    pub fn set_at_risk(&self) {
        self.at_risk.store(true, Ordering::Release);
    }

    pub fn is_committed(&self) -> bool {
        self.committed.load(Ordering::Acquire)
    }

    pub fn set_committed(&self) {
        self.committed.store(true, Ordering::Release);
    }

    pub fn is_checked(&self) -> bool {
        self.checked.load(Ordering::Acquire)
    }

    pub fn set_checked(&self, val: bool) {
        self.checked.store(val, Ordering::Release);
    }

    pub fn is_cleaned(&self) -> bool {
        self.cleaned.load(Ordering::Acquire)
    }

    pub fn set_cleaned(&self) {
        self.cleaned.store(true, Ordering::Release);
    }

    pub fn set_abort_through(&self, id: usize) {
        unsafe { *self.abort_through.get() = id };
    }
    pub fn get_abort_through(&self) -> usize {
        unsafe { *self.abort_through.get() }
    }
}

impl std::fmt::Debug for MsgEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        use MsgEdge::*;
        match &*self {
            ReadWrite(txn_id) => write!(f, "{}", format!("[rw: {}]", txn_id)),
            WriteWrite(txn_id) => write!(f, "{}", format!("[ww: {}]", txn_id)),
            WriteRead(txn_id) => write!(f, "{}", format!("[wr: {}]", txn_id)),
        }
    }
}

impl fmt::Display for MsgNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[t-id: {}]", format!("{}", self.get_id().unwrap()),)
    }
}
