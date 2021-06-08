//use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use parking_lot::Mutex;
use spin::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use rustc_hash::FxHashSet;
use std::cell::UnsafeCell;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};

pub fn from_usize<'a>(address: usize) -> &'a RwNode<'a> {
    // Safety: finding an address in some access history implies the corresponding node is either:
    // (i) pinned on another thread, so it is save to give out reference to it.
    // (ii) scheduled for deletion by another thread, again we can safely give out a reference, as it won't be destroyed
    // until after this thread is unpinned.
    unsafe { &*(address as *const RwNode<'a>) }
}

pub fn to_usize<'a>(node: Box<RwNode<'a>>) -> usize {
    let raw: *mut RwNode = Box::into_raw(node);
    raw as usize
}

pub fn to_box<'a>(address: usize) -> Box<RwNode<'a>> {
    // Safety: a node is owned by a single thread, so this method is only called once in order to pass the node to the
    // epoch based garbage collector.
    unsafe {
        let raw = address as *mut RwNode<'a>;
        Box::from_raw(raw)
    }
}

pub fn ref_to_usize<'a>(node: &'a RwNode<'a>) -> usize {
    let ptr: *const RwNode<'a> = node;
    ptr as usize
}

pub type EdgeSet<'a> = Mutex<FxHashSet<Edge<'a>>>;

#[derive(Debug, Clone)]
pub enum Edge<'a> {
    ReadWrite(&'a RwNode<'a>),
    Other(&'a RwNode<'a>),
}

// #[derive(Debug)]
// pub struct RwNode<'a> {
//     //    node: RwLock<Node<'a>>,
//     node: Node<'a>,
// }

#[derive(Debug)]
pub struct RwNode<'a> {
    incoming: UnsafeCell<Option<EdgeSet<'a>>>,
    outgoing: UnsafeCell<Option<EdgeSet<'a>>>,
    committed: AtomicBool,
    cascading_abort: AtomicBool,
    aborted: AtomicBool,
    cleaned: AtomicBool,
    checked: AtomicBool,
    lock: RwLock<u32>,
}

unsafe impl<'a> Send for RwNode<'a> {}
unsafe impl<'a> Sync for RwNode<'a> {}

// impl<'a> RwNode<'a> {
//     pub fn new() -> Self {
//         Self {
//             //            node: RwLock::new(Node::new()),
//             node: Node::new(),
//         }
//     }

//     pub fn new_with_sets(incoming: EdgeSet<'a>, outgoing: EdgeSet<'a>) -> Self {
//         Self {
//             //            node: RwLock::new(Node::new_with_sets(incoming, outgoing)),
//             node: Node::new_with_sets(incoming, outgoing),
//         }
//     }

//     //    pub fn read(&self) -> RwLockReadGuard<Node<'a>> {

//     pub fn read(&self) -> RwLockReadGuard<u32> {
//         self.node.lock.read()
//     }

//     //    pub fn write(&self) -> RwLockWriteGuard<Node<'a>> {
//     pub fn write(&self) -> RwLockWriteGuard<u32> {
//         self.node.lock.write()
//     }
// }

impl<'a> RwNode<'a> {
    pub fn read(&self) -> RwLockReadGuard<u32> {
        self.lock.read()
    }

    //    pub fn write(&self) -> RwLockWriteGuard<Node<'a>> {
    pub fn write(&self) -> RwLockWriteGuard<u32> {
        self.lock.write()
    }

    pub fn new() -> Self {
        Self {
            incoming: UnsafeCell::new(Some(Mutex::new(FxHashSet::default()))),
            outgoing: UnsafeCell::new(Some(Mutex::new(FxHashSet::default()))),
            committed: AtomicBool::new(false),
            cascading_abort: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
            cleaned: AtomicBool::new(false),
            checked: AtomicBool::new(false),
            lock: RwLock::new(0),
        }
    }

    pub fn new_with_sets(incoming: EdgeSet<'a>, outgoing: EdgeSet<'a>) -> Self {
        Self {
            incoming: UnsafeCell::new(Some(incoming)),
            outgoing: UnsafeCell::new(Some(outgoing)),
            committed: AtomicBool::new(false),
            cascading_abort: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
            cleaned: AtomicBool::new(false),
            checked: AtomicBool::new(false),
            lock: RwLock::new(0),
        }
    }

    pub fn incoming_edge_exists(&self, from: &'a RwNode<'a>) -> bool {
        let guard = unsafe {
            self.incoming
                .get()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .lock()
        };
        let a = Edge::Other(from);
        let b = Edge::ReadWrite(from);
        let exists = guard.contains(&a) || guard.contains(&b);
        drop(guard);
        exists
    }

    pub fn is_incoming(&self) -> bool {
        let guard = unsafe {
            self.incoming
                .get()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .lock()
        };
        let emp = !guard.is_empty();

        drop(guard);

        emp
    }

    pub fn insert_incoming(&self, from_node: &'a RwNode<'a>, rw_edge: bool) {
        let mut guard = unsafe {
            self.incoming
                .get()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .lock()
        };
        let edge;
        if rw_edge {
            edge = Edge::ReadWrite(from_node);
        } else {
            edge = Edge::Other(from_node);
        }
        guard.insert(edge);
        drop(guard);
    }

    pub fn remove_incoming(&self, from: &Edge<'a>) {
        let mut guard = unsafe {
            self.incoming
                .get()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .lock()
        };
        guard.remove(from);
        drop(guard);
    }

    pub fn insert_outgoing(&self, to_node: &'a RwNode<'a>, rw_edge: bool) {
        let mut guard = unsafe {
            self.outgoing
                .get()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .lock()
        };
        let edge;
        if rw_edge {
            edge = Edge::ReadWrite(to_node);
        } else {
            edge = Edge::Other(to_node);
        }
        guard.insert(edge);
        drop(guard);
    }

    pub fn clear_incoming(&self) {
        let mut guard = unsafe {
            self.incoming
                .get()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .lock()
        };
        guard.clear();
        drop(guard);
    }

    pub fn clear_outgoing(&self) {
        let mut guard = unsafe {
            self.outgoing
                .get()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .lock()
        };
        guard.clear();
        drop(guard);
    }

    pub fn take_incoming(&self) -> EdgeSet<'a> {
        unsafe { self.incoming.get().as_mut().unwrap().take().unwrap() }
    }

    pub fn take_outgoing(&self) -> EdgeSet<'a> {
        unsafe { self.outgoing.get().as_mut().unwrap().take().unwrap() }
    }

    pub fn get_outgoing(&self) -> FxHashSet<Edge<'a>> {
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
}

impl<'a> PartialEq for Edge<'a> {
    fn eq(&self, other: &Self) -> bool {
        use Edge::*;

        match (self, other) {
            (&ReadWrite(a), &ReadWrite(b)) => ptr::eq(a, b),
            (&Other(a), &Other(b)) => ptr::eq(a, b),
            _ => false,
        }
    }
}

impl<'a> Eq for Edge<'a> {}

impl<'a> Hash for Edge<'a> {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        use Edge::*;

        match self {
            ReadWrite(node) => {
                let id = ref_to_usize(node);
                id.hash(hasher)
            }
            Other(node) => {
                let id = ref_to_usize(node);
                id.hash(hasher)
            }
        }
    }
}

impl<'a> fmt::Display for Edge<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Edge::ReadWrite(node) => writeln!(f, "rw: {}", ref_to_usize(node)).unwrap(),
            Edge::Other(node) => writeln!(f, "other: {}", ref_to_usize(node)).unwrap(),
        }

        Ok(())
    }
}

impl<'a> fmt::Display for RwNode<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut incoming = String::new();
        let empty = unsafe {
            self.incoming
                .get()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .lock()
                .is_empty()
        };
        if !empty {
            incoming.push('[');
            let g = unsafe {
                self.incoming
                    .get()
                    .as_ref()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .lock()
            };
            for edge in &*g {
                incoming.push_str(&format!("{}", edge));
                incoming.push_str(", ");
            }
            drop(g);
            incoming.push_str(&format!("]"));
        } else {
            incoming.push_str("[]");
        }

        writeln!(f).unwrap();
        writeln!(f, "incoming: {}", incoming).unwrap();
        writeln!(f, "committed: {:?}", self.committed).unwrap();
        writeln!(f, "cascading_abort: {:?}", self.cascading_abort).unwrap();
        writeln!(f, "aborted: {:?}", self.aborted).unwrap();
        writeln!(f, "cleaned: {:?}", self.cleaned).unwrap();
        write!(f, "checked: {:?}", self.checked).unwrap();
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
        let e1 = Edge::ReadWrite(&node);
        let e2 = Edge::ReadWrite(&node);
        assert_eq!(e1, e2);

        let e3 = Edge::Other(&node);
        assert!(e3 != e1);

        let onode = RwNode::new();
        let e4 = Edge::ReadWrite(&onode);
        assert!(e1 != e4);
    }

    #[test]
    fn node() {
        let node1 = RwNode::new();
        let node2 = RwNode::new();

        node1.read().insert_incoming(&node2, true);
        node1.read().insert_incoming(&node2, true);
        node1.read().insert_incoming(&node2, false);
        assert_eq!(node1.read().is_incoming(), true);

        let edge = Edge::ReadWrite(&node2);
        node1.read().remove_incoming(&edge);
        assert_eq!(node1.read().is_incoming(), true);

        let edge = Edge::Other(&node2);
        node1.read().remove_incoming(&edge);
        assert_eq!(node1.read().is_incoming(), false);
    }
}
