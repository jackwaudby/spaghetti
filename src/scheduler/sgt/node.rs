use parking_lot::Mutex;
use rustc_hash::FxHashSet;
use spin::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use std::cell::UnsafeCell;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

unsafe impl<'a> Send for Node {}
unsafe impl<'a> Sync for Node {}

pub type EdgeSet = Mutex<FxHashSet<Edge>>;

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum Edge {
    ReadWrite(usize),
    WriteWrite(usize),
    WriteRead(usize),
}

#[derive(Debug)]
pub struct Node {
    pub thread_id: usize,
    thread_ctr: usize,
    node_id: UnsafeCell<Option<usize>>,
    incoming: UnsafeCell<Option<EdgeSet>>,
    outgoing: UnsafeCell<Option<EdgeSet>>,
    complete: AtomicBool,
    committed: AtomicBool,
    cascading: AtomicBool,
    aborted: AtomicBool,
    cleaned: AtomicBool,
    checked: AtomicBool,
    lock: RwLock<u32>,
}

impl Node {
    pub fn read(&self) -> RwLockReadGuard<u32> {
        self.lock.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<u32> {
        self.lock.write()
    }

    pub fn new(thread_id: usize, thread_ctr: usize, incoming: EdgeSet, outgoing: EdgeSet) -> Self {
        Self {
            thread_id,
            thread_ctr,
            node_id: UnsafeCell::new(None),
            incoming: UnsafeCell::new(Some(incoming)),
            outgoing: UnsafeCell::new(Some(outgoing)),
            committed: AtomicBool::new(false),
            cascading: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
            cleaned: AtomicBool::new(false),
            checked: AtomicBool::new(false),
            complete: AtomicBool::new(false),
            lock: RwLock::new(0),
        }
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
                    assert_eq!(guard.remove(from), true);
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

    pub fn is_cleaned(&self) -> bool {
        self.cleaned.load(Ordering::Acquire)
    }

    pub fn set_cleaned(&self) {
        self.cleaned.store(true, Ordering::Release);
    }

    pub fn is_complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }

    pub fn set_complete(&self) {
        self.complete.store(true, Ordering::Release);
    }

    pub fn print_edges(&self, incoming: bool) -> String {
        let mut res = String::new();

        let edge_set = if incoming {
            unsafe { self.incoming.get().as_ref().unwrap().as_ref() }
        } else {
            unsafe { self.outgoing.get().as_ref().unwrap().as_ref() }
        };

        match edge_set {
            // incoming edge not removed
            Some(edges) => {
                let guard = edges.lock(); // lock edge set

                // not removed but empty
                if guard.is_empty() {
                    res.push_str("[ ]");
                } else {
                    // contains edges
                    res.push_str(&format!("["));

                    for edge in &*guard {
                        res.push_str(&format!("{}", edge));
                        res.push_str(", ");
                    }
                    res.pop(); // remove trailing ', '
                    res.pop();
                    res.push_str(&format!("]"));
                }

                drop(guard);
            }
            // edge set has been removed
            None => res.push_str("[cleared]"),
        };

        res
    }

    // pub fn depth_first_search(&self, incoming: bool) -> FxHashSet<usize> {
    //     let mut visited = FxHashSet::default(); // nodes that have been visited
    //     let mut stack = Vec::new(); // nodes left to visit

    //     let edges;
    //     if incoming {
    //         edges = self.get_incoming(); // start nodes to visit
    //     } else {
    //         edges = self.get_outgoing(); // start nodes to visit
    //     }

    //     let mut inc = edges.into_iter().collect();
    //     stack.append(&mut inc); // push to stack

    //     while let Some(edge) = stack.pop() {
    //         let current = match edge {
    //             Edge::ReadWrite(node) => node,
    //             Edge::WriteWrite(node) => node,
    //             Edge::WriteRead(node) => node,
    //         };

    //         if visited.contains(&current) {
    //             continue; // already visited
    //         }

    //         visited.insert(current);

    //         let current_ref = from_usize(current);
    //         let edges;
    //         if incoming {
    //             edges = current_ref.get_incoming(); // start nodes to visit
    //         } else {
    //             edges = current_ref.get_outgoing(); // start nodes to visit
    //         }

    //         let mut inc = edges.into_iter().collect();
    //         stack.append(&mut inc);
    //     }
    //     visited
    // }
}

impl fmt::Display for Edge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Edge::ReadWrite(id) => write!(f, "rw:{}", id).unwrap(),
            Edge::WriteWrite(id) => write!(f, "ww:{}", id,).unwrap(),
            Edge::WriteRead(id) => write!(f, "wr:{}", id,).unwrap(),
        }

        Ok(())
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // let mut nodes_in = self.depth_first_search(true); // nodes found from incoming edges
        //  let mut nodes_out = self.depth_first_search(false); // nodes found from incoming edges

        let ptr: *const Node = self;
        let id = ptr as usize;
        // nodes_in.remove(&id);
        //     nodes_out.remove(&id);

        //     let nodes: FxHashSet<_> = nodes_in.union(&nodes_out).collect();

        writeln!(f).unwrap();
        writeln!(f, "-------------------------------------------------------------------------------------------").unwrap();
        writeln!(f, "thread id: {:?}", self.thread_id).unwrap();
        writeln!(f, "thread ctr: {:?}", self.thread_ctr).unwrap();
        writeln!(f, "expected ref id: {}", unsafe {
            self.node_id.get().as_ref().unwrap().unwrap()
        })
        .unwrap();
        writeln!(f, "actual ref id: {}", id).unwrap();
        writeln!(f, "incoming: {}", self.print_edges(true)).unwrap();
        writeln!(f, "outgoing: {}", self.print_edges(false)).unwrap();

        writeln!(
            f,
            "committed: {:?}, cascading: {:?}, aborted: {:?}, cleaned: {:?}, checked: {:?}, complete: {:?}",
            self.committed, self.cascading, self.aborted, self.cleaned, self.checked, self.complete
        )
        .unwrap();
        writeln!(f, "-------------------------------------------------------------------------------------------").unwrap();
        writeln!(f).unwrap();

        // for node in nodes_in.iter() {
        //     let n = from_usize(*node);

        //     writeln!(f, "-------------------------------------------------------------------------------------------").unwrap();
        //     writeln!(f).unwrap();
        //     writeln!(f, "thread id: {:?}", n.thread_id).unwrap();
        //     writeln!(f, "thread ctr: {:?}", n.thread_ctr).unwrap();
        //     writeln!(f, "expected ref id: {:?}", unsafe {
        //         n.node_id.get().as_ref().unwrap()
        //     })
        //     .unwrap();
        //     writeln!(f, "actual ref id: {}", node).unwrap();

        //     writeln!(f, "incoming: {}", n.print_edges(true)).unwrap();
        //     writeln!(f, "outgoing: {}", n.print_edges(false)).unwrap();

        //     writeln!(
        //         f,
        //         "committed: {:?}, cascading: {:?}, aborted: {:?}, cleaned: {:?}, checked: {:?}, complete: {:?}",
        //         n.committed, n.cascading_abort, n.aborted, n.cleaned, n.checked, n.complete
        //     )
        //     .unwrap();
        //     writeln!(f, "-------------------------------------------------------------------------------------------").unwrap();
        //     writeln!(f).unwrap();
        // }

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
