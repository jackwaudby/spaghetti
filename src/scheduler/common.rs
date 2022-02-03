use crate::workloads::IsolationLevel;

use parking_lot::Mutex;
use rustc_hash::FxHashSet;
use spin::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use std::cell::UnsafeCell;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

unsafe impl<'a> Send for Node {}
unsafe impl<'a> Sync for Node {}

pub type EdgeSet = Mutex<FxHashSet<Edge>>;

// (from/to, thread id)
#[derive(Clone, Eq, Hash, PartialEq)]
pub enum Edge {
    ReadWrite(usize),
    WriteWrite(usize),
    WriteRead(usize),
}

#[derive(Debug)]
pub enum Incoming {
    None,
    SomeRelevant,
    SomeNotRelevant,
}

#[derive(Debug)]
pub struct Node {
    thread_id: usize,
    thread_ctr: usize,
    isolation_level: Option<IsolationLevel>,
    node_id: UnsafeCell<Option<usize>>,
    incoming: UnsafeCell<Option<EdgeSet>>,
    outgoing: UnsafeCell<Option<EdgeSet>>,
    committed: AtomicBool,
    cascading: AtomicBool,
    aborted: AtomicBool,
    cleaned: AtomicBool,
    checked: AtomicBool,
    early: AtomicBool,
    lock: RwLock<u32>,
}

impl Node {
    pub fn read(&self) -> RwLockReadGuard<u32> {
        self.lock.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<u32> {
        self.lock.write()
    }

    pub fn new(
        thread_id: usize,
        thread_ctr: usize,
        incoming: EdgeSet,
        outgoing: EdgeSet,
        isolation_level: Option<IsolationLevel>,
    ) -> Self {
        Self {
            thread_id,
            thread_ctr,
            isolation_level,
            node_id: UnsafeCell::new(None),
            incoming: UnsafeCell::new(Some(incoming)),
            outgoing: UnsafeCell::new(Some(outgoing)),
            committed: AtomicBool::new(false),
            cascading: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
            cleaned: AtomicBool::new(false),
            checked: AtomicBool::new(false),
            early: AtomicBool::new(false),
            lock: RwLock::new(0),
        }
    }

    pub fn get_isolation_level(&self) -> IsolationLevel {
        self.isolation_level.unwrap()
    }

    pub fn get_thread_ctr(&self) -> usize {
        self.thread_ctr
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

    pub fn msgt_has_incoming(&self) -> Incoming {
        let incoming = unsafe { self.incoming.get().as_ref() };

        match incoming {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let guard = edges.lock();

                    if guard.is_empty() {
                        drop(guard);
                        Incoming::None
                    } else {
                        match self.isolation_level.unwrap() {
                            IsolationLevel::ReadUncommitted => {
                                if guard.iter().any(|x| variant_eq(x, &Edge::WriteWrite(0))) {
                                    Incoming::SomeRelevant
                                } else {
                                    Incoming::SomeNotRelevant
                                }
                            }
                            IsolationLevel::ReadCommitted => {
                                if guard.iter().any(|x| {
                                    variant_eq(x, &Edge::WriteWrite(0))
                                        || variant_eq(x, &Edge::WriteRead(0))
                                }) {
                                    Incoming::SomeRelevant
                                } else {
                                    Incoming::SomeNotRelevant
                                }
                            }
                            IsolationLevel::Serializable => Incoming::SomeRelevant,
                        }
                    }
                }
                None => panic!("incoming edge set already cleaned"),
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

    /// Insert an outgoing edge: (this) --> (to)
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

    pub fn get_id(&self) -> usize {
        unsafe { self.node_id.get().as_mut().unwrap().unwrap() }
    }

    pub fn get_thread_id(&self) -> usize {
        self.thread_id
    }

    pub fn is_aborted(&self) -> bool {
        self.aborted.load(Ordering::Acquire)
    }

    pub fn set_aborted(&self) {
        self.aborted.store(true, Ordering::Release);
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

    pub fn is_early(&self) -> bool {
        self.early.load(Ordering::Acquire)
    }

    pub fn set_early(&self) {
        self.early.store(true, Ordering::Release);
    }

    pub fn is_cleaned(&self) -> bool {
        self.cleaned.load(Ordering::Acquire)
    }

    pub fn set_cleaned(&self) {
        self.cleaned.store(true, Ordering::Release);
    }
}

fn variant_eq(a: &Edge, b: &Edge) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

impl std::fmt::Debug for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        use Edge::*;
        match &*self {
            ReadWrite(txn_id) => write!(f, "{}", format!("[rw: {:x}]", txn_id)),
            WriteWrite(txn_id) => write!(f, "{}", format!("[ww: {:x}]", txn_id)),
            WriteRead(txn_id) => write!(f, "{}", format!("[wr: {:x}]", txn_id)),
        }
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "[th-id: {}, t-id: {}]",
            // "[th-id: {}, t-id: {}, iso: {}]",
            self.get_thread_id(),
            format!("{:x}", self.get_id()),
            // self.get_isolation_level(),
        )
    }
}
