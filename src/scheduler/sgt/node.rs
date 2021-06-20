use parking_lot::Mutex;
use rustc_hash::FxHashSet;
use spin::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use std::cell::UnsafeCell;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

pub fn from_usize<'a>(address: usize) -> &'a RwNode {
    // Safety: finding an address in some access history implies the corresponding node is either:
    // (i) pinned on another thread, so it is save to give out reference to it.
    // (ii) scheduled for deletion by another thread, again we can safely give out a reference, as it won't be destroyed
    // until after this thread is unpinned.
    unsafe { &*(address as *const RwNode) }
}

pub fn to_usize(node: Box<RwNode>) -> usize {
    let raw: *mut RwNode = Box::into_raw(node);
    raw as usize
}

pub fn to_box(address: usize) -> Box<RwNode> {
    // Safety: a node is owned by a single thread, so this method is only called once in order to pass the node to the
    // epoch based garbage collector.
    unsafe {
        let raw = address as *mut RwNode;
        Box::from_raw(raw)
    }
}

pub fn ref_to_usize<'a>(node: &'a RwNode) -> usize {
    let ptr: *const RwNode = node;
    ptr as usize
}

pub type EdgeSet = Mutex<FxHashSet<Edge>>;

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum Edge {
    ReadWrite(usize),
    WriteWrite(usize),
    WriteRead(usize),
}

#[derive(Debug)]
pub struct RwNode {
    pub incoming: UnsafeCell<Option<EdgeSet>>,
    outgoing: UnsafeCell<Option<EdgeSet>>,
    committed: AtomicBool,
    cascading_abort: AtomicBool,
    aborted: AtomicBool,
    cleaned: AtomicBool,
    checked: AtomicBool,
    complete: AtomicBool,
    lock: RwLock<u32>,
    pub inserted: UnsafeCell<Vec<String>>,
    pub removed: UnsafeCell<Vec<Edge>>,
    pub skipped: UnsafeCell<Vec<Edge>>,
    pub cleared: UnsafeCell<Option<FxHashSet<Edge>>>,
}

unsafe impl<'a> Send for RwNode {}
unsafe impl<'a> Sync for RwNode {}

impl RwNode {
    pub fn read(&self) -> RwLockReadGuard<u32> {
        self.lock.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<u32> {
        self.lock.write()
    }

    pub fn new() -> Self {
        Self {
            incoming: UnsafeCell::new(Some(Mutex::new(FxHashSet::default()))),
            outgoing: UnsafeCell::new(Some(Mutex::new(FxHashSet::default()))),
            removed: UnsafeCell::new(Vec::new()),
            skipped: UnsafeCell::new(Vec::new()),
            inserted: UnsafeCell::new(Vec::new()),
            committed: AtomicBool::new(false),
            cascading_abort: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
            cleaned: AtomicBool::new(false),
            checked: AtomicBool::new(false),
            complete: AtomicBool::new(false),
            lock: RwLock::new(0),
            cleared: UnsafeCell::new(None),
        }
    }

    pub fn new_with_sets(incoming: EdgeSet, outgoing: EdgeSet) -> Self {
        Self {
            incoming: UnsafeCell::new(Some(incoming)),
            outgoing: UnsafeCell::new(Some(outgoing)),
            removed: UnsafeCell::new(Vec::new()),
            inserted: UnsafeCell::new(Vec::new()),
            skipped: UnsafeCell::new(Vec::new()),
            committed: AtomicBool::new(false),
            cascading_abort: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
            cleaned: AtomicBool::new(false),
            checked: AtomicBool::new(false),
            complete: AtomicBool::new(false),
            lock: RwLock::new(0),
            cleared: UnsafeCell::new(None),
        }
    }

    /// Returns `true` if an edge from a given node exists in this node's edge set.
    pub fn incoming_edge_exists(&self, from: &Edge) -> bool {
        // Safety: the incoming edge field is only mutated by a single thread during the cleanup() operation.
        // Additonally, this method is only called by the same single thread.
        let incoming = unsafe { self.incoming.get().as_ref() };

        match incoming {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let guard = edges.lock();
                    let exists = guard.contains(from);
                    drop(guard);
                    exists
                }
                None => panic!("incoming edge set already cleaned"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Returns `true` if the node has at least 1 incoming edge.
    pub fn is_incoming(&self) -> bool {
        // Safety: the incoming edge field is only mutated by a single thread during the cleanup() operation.
        // Additonally, this method is only called by the same single thread during the check_committed() operation.
        let incoming = unsafe { self.incoming.get().as_ref() };

        match incoming {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let guard = edges.lock();
                    let res = guard.is_empty();

                    for edge in guard.iter() {
                        let from_id = match edge {
                            Edge::ReadWrite(node) => node,
                            Edge::WriteWrite(node) => node,
                            Edge::WriteRead(node) => node,
                        };

                        let from_ref = from_usize(*from_id);

                        let ptr: *const RwNode = self;
                        let id = ptr as usize;

                        assert!(
                            !from_ref.is_complete(),
                            "{} has an incoming edge {} from a completed node {}! {}",
                            id,
                            edge,
                            from_id,
                            from_ref
                        );
                    }

                    drop(guard);
                    !res
                }
                None => panic!("incoming edge set already cleaned"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Insert an incoming edge from a node.
    pub fn insert_incoming(&self, from: Edge) {
        // Assert: should not be attempting to insert an edge is transaction has terminated.
        assert!(!self.is_aborted());
        assert!(!self.is_committed());

        // Safety: the incoming edge field is only mutated by a single thread during the cleanup() operation.
        let incoming = unsafe { self.incoming.get().as_ref() };

        match incoming {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let mut guard = edges.lock();

                    guard.insert(from);

                    drop(guard);
                }
                None => panic!("incoming edge set already cleaned"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Remove an edge from this node's incoming edge set.
    pub fn remove_incoming(&self, from: &Edge) {
        // Assert: should not be attempting to insert an removed is transaction has terminated.
        assert!(!self.is_complete());

        // Safety: the incoming edge field is only mutated by a single thread during the cleanup() operation.
        let incoming = unsafe { self.incoming.get().as_ref() };

        match incoming {
            Some(edge_set) => match edge_set {
                Some(edges) => {
                    let mut guard = edges.lock();
                    assert_eq!(guard.remove(from), true);
                    drop(guard);
                }
                None => panic!("incoming edge set already cleaned"),
            },
            None => panic!("check unsafe"),
        }
    }

    /// Insert an outgoing edge from a node.
    pub fn insert_outgoing(&self, to: Edge) {
        // Safety: the incoming edge field is only mutated by a single thread during the cleanup() operation.
        let outgoing = unsafe { self.outgoing.get().as_ref() };

        match outgoing {
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
        let guard = unsafe {
            self.outgoing
                .get()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .lock()
        };
        let out = guard.clone();
        drop(guard);
        out
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

    pub fn is_aborted(&self) -> bool {
        self.aborted.load(Ordering::Acquire)
    }

    pub fn set_aborted(&self) {
        self.aborted.store(true, Ordering::Release);
    }

    pub fn is_cascading_abort(&self) -> bool {
        self.cascading_abort.load(Ordering::Acquire)
    }

    pub fn set_cascading_abort(&self) {
        self.cascading_abort.store(true, Ordering::Release);
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

    pub fn depth_first_search(&self) -> FxHashSet<usize> {
        let mut visited = FxHashSet::default(); // nodes that have been visited
        let mut stack = Vec::new(); // nodes left to visit

        let incoming = self.get_incoming(); // start nodes to visit
        let mut inc = incoming.into_iter().collect();
        stack.append(&mut inc); // push to stack

        while let Some(edge) = stack.pop() {
            let current = match edge {
                Edge::ReadWrite(node) => node,
                Edge::WriteWrite(node) => node,
                Edge::WriteRead(node) => node,
            };

            if visited.contains(&current) {
                continue; // already visited
            }

            visited.insert(current);

            let current_ref = from_usize(current);
            let incoming = current_ref.get_incoming();
            let mut inc = incoming.into_iter().collect();
            stack.append(&mut inc);
        }
        visited
    }
}

impl fmt::Display for Edge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Edge::ReadWrite(id) => write!(f, "rw:{}", id).unwrap(),
            Edge::WriteWrite(id) => write!(f, "ww:{}", id).unwrap(),
            Edge::WriteRead(id) => write!(f, "wr:{}", id).unwrap(),
        }

        Ok(())
    }
}

impl fmt::Display for RwNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut nodes = self.depth_first_search(); // nodes found from incoming edges

        let ptr: *const RwNode = self;
        let id = ptr as usize;
        nodes.remove(&id);

        writeln!(f).unwrap();
        writeln!(f, "-------------------------------------------------------------------------------------------").unwrap();
        writeln!(f, "id: {}", id).unwrap();
        writeln!(f, "incoming: {}", self.print_edges(true)).unwrap();
        writeln!(f, "outgoing: {}", self.print_edges(false)).unwrap();
        writeln!(f, "removed: {:?}", unsafe {
            self.removed.get().as_mut().unwrap()
        })
        .unwrap();
        writeln!(f, "skipped: {:?}", unsafe {
            self.skipped.get().as_mut().unwrap()
        })
        .unwrap();
        writeln!(f, "cleared: {:?}", unsafe {
            self.cleared.get().as_mut().unwrap()
        })
        .unwrap();

        writeln!(
            f,
            "committed: {:?}, cascading: {:?}, aborted: {:?}, cleaned: {:?}, checked: {:?}, complete: {:?}",
            self.committed, self.cascading_abort, self.aborted, self.cleaned, self.checked, self.complete
        )
        .unwrap();
        writeln!(f, "-------------------------------------------------------------------------------------------").unwrap();
        writeln!(f).unwrap();

        for node in nodes.iter() {
            let n = from_usize(*node);

            writeln!(f, "-------------------------------------------------------------------------------------------").unwrap();
            writeln!(f).unwrap();
            writeln!(f, "id: {}", node).unwrap();
            writeln!(f, "incoming: {}", n.print_edges(true)).unwrap();
            writeln!(f, "outgoing: {}", n.print_edges(false)).unwrap();
            writeln!(
                f,
                "committed: {:?}, cascading: {:?}, aborted: {:?}, cleaned: {:?}, checked: {:?}, complete: {:?}",
                n.committed, n.cascading_abort, n.aborted, n.cleaned, n.checked, n.complete
            )
            .unwrap();
            writeln!(f, "-------------------------------------------------------------------------------------------").unwrap();
            writeln!(f).unwrap();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utils() {
        let node = RwNode::new();
        let boxed = Box::new(node);
        let id = to_usize(boxed);
        let ref_node = from_usize(id);
        let ref_id = ref_to_usize(ref_node);
        assert_eq!(id, ref_id);

        let n1 = RwNode::new();
        let nr1 = &n1;
        let nr2 = nr1.clone();
        let nr3 = &nr2;

        assert_eq!(ref_to_usize(nr1), ref_to_usize(&nr2));
        assert_eq!(ref_to_usize(nr1), ref_to_usize(nr3));
    }

    #[test]
    fn edge() {
        let node = RwNode::new();
        let boxed = Box::new(node);
        let id = to_usize(boxed);

        let e1 = Edge::ReadWrite(id);
        let e2 = Edge::ReadWrite(id);
        assert_eq!(e1, e2);

        let e3 = Edge::WriteWrite(id);
        assert!(e3 != e1);

        let onode = RwNode::new();
        let oboxed = Box::new(onode);
        let oid = to_usize(oboxed);

        let e4 = Edge::ReadWrite(oid);
        let e5 = Edge::WriteWrite(oid);

        assert!(e1 != e4);
        assert!(e1 != e5);
        assert!(e4 != e5);
    }

    #[test]
    fn node() {
        let n1 = RwNode::new();
        let id1 = to_usize(Box::new(n1));
        let node1 = from_usize(id1);

        let n2 = RwNode::new();
        let id2 = to_usize(Box::new(n2));

        node1.insert_incoming(Edge::ReadWrite(id2));
        node1.insert_incoming(Edge::WriteWrite(id2));

        assert_eq!(node1.is_incoming(), true);

        node1.remove_incoming(&Edge::ReadWrite(id2));
        assert_eq!(node1.is_incoming(), true);

        let edge = Edge::WriteWrite(id2);
        node1.remove_incoming(&edge);
        assert_eq!(node1.is_incoming(), false);
    }

    #[test]
    fn dfs() {
        let n1 = RwNode::new();
        let id1 = to_usize(Box::new(n1));
        let node1 = from_usize(id1);

        let n2 = RwNode::new();
        let id2 = to_usize(Box::new(n2));
        let node2 = from_usize(id2);

        let n3 = RwNode::new();
        let id3 = to_usize(Box::new(n3));
        let node3 = from_usize(id3);

        node2.insert_incoming(Edge::WriteWrite(id1));

        node3.insert_incoming(Edge::WriteWrite(id2));

        node1.insert_incoming(Edge::WriteWrite(id3));

        let mut res = FxHashSet::default();
        res.insert(id1);
        res.insert(id2);
        res.insert(id3);

        assert_eq!(node1.depth_first_search(), res);
    }
}
