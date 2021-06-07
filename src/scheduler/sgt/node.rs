use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use rustc_hash::FxHashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::debug;

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

type NodeSet<'a> = Mutex<FxHashSet<Edge<'a>>>;

#[derive(Debug, Clone)]
pub enum Edge<'a> {
    ReadWrite(&'a RwNode<'a>),
    Other(&'a RwNode<'a>),
}

impl<'a> PartialEq for Edge<'a> {
    fn eq(&self, other: &Self) -> bool {
        use Edge::*;

        match (self, other) {
            (&ReadWrite(ref a), &ReadWrite(ref b)) => ptr::eq(a, b),
            (&Other(ref a), &Other(ref b)) => ptr::eq(a, b),
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

#[derive(Debug)]
pub struct RwNode<'a> {
    node: RwLock<Node<'a>>,
}

impl<'a> RwNode<'a> {
    pub fn new() -> Self {
        Self {
            node: RwLock::new(Node::new()),
        }
    }

    pub fn read(&self) -> RwLockReadGuard<Node<'a>> {
        self.node.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<Node<'a>> {
        self.node.write()
    }
}

#[derive(Debug)]
pub struct Node<'a> {
    incoming: NodeSet<'a>,
    outgoing: NodeSet<'a>,
    committed: AtomicBool,
    cascading_abort: AtomicBool,
    aborted: AtomicBool,
    cleaned: AtomicBool,
    checked: AtomicBool,
}

impl<'a> Node<'a> {
    pub fn new() -> Self {
        Self {
            incoming: Mutex::new(FxHashSet::default()),
            outgoing: Mutex::new(FxHashSet::default()),
            committed: AtomicBool::new(false),
            cascading_abort: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
            cleaned: AtomicBool::new(false),
            checked: AtomicBool::new(false),
        }
    }

    pub fn incoming_edge_exists(&self, from: &'a RwNode<'a>) -> bool {
        let guard = self.incoming.lock();
        let a = Edge::Other(from);
        let b = Edge::ReadWrite(from);
        let exists = guard.contains(&a) || guard.contains(&b);
        drop(guard);
        exists
    }

    pub fn is_incoming(&self) -> bool {
        let guard = self.incoming.lock();
        let emp = !guard.is_empty();
        drop(guard);
        debug!("has incoming: {}", emp);
        emp
    }

    pub fn insert_incoming(&self, from_node: &'a RwNode<'a>, rw_edge: bool) {
        let mut guard = self.incoming.lock();
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
        let mut guard = self.incoming.lock();
        guard.remove(from);
        drop(guard);
    }

    pub fn insert_outgoing(&self, to_node: &'a RwNode<'a>, rw_edge: bool) {
        let mut guard = self.outgoing.lock();
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
        let mut guard = self.incoming.lock();
        guard.clear();
        drop(guard);
    }

    pub fn clear_outgoing(&self) {
        let mut guard = self.outgoing.lock();
        guard.clear();
        drop(guard);
    }

    pub fn take_incoming(&mut self) -> FxHashSet<Edge<'a>> {
        let g = self.incoming.lock();
        let res = g.clone();
        drop(g);
        res
    }

    pub fn take_outgoing(&mut self) -> FxHashSet<Edge<'a>> {
        let g = self.outgoing.lock();
        let res = g.clone();
        drop(g);
        res
    }

    // pub fn take_outgoing(&mut self) -> Mutex<Vec<(&RwNode, bool)>> {
    //     self.outgoing.take().unwrap()
    // }

    pub fn get_outgoing(&self) -> FxHashSet<Edge<'a>> {
        let guard = self.outgoing.lock();
        let out = guard.clone();
        drop(guard);
        out
    }

    pub fn is_aborted(&self) -> bool {
        self.aborted.load(Ordering::SeqCst)
    }

    pub fn set_aborted(&self) {
        self.aborted.store(true, Ordering::SeqCst);
    }

    pub fn is_cascading_abort(&self) -> bool {
        self.cascading_abort.load(Ordering::SeqCst)
    }

    pub fn set_cascading_abort(&self) {
        self.cascading_abort.store(true, Ordering::SeqCst);
    }

    pub fn is_committed(&self) -> bool {
        self.committed.load(Ordering::SeqCst)
    }

    pub fn set_committed(&self) {
        self.committed.store(true, Ordering::SeqCst);
    }

    pub fn is_checked(&self) -> bool {
        self.checked.load(Ordering::SeqCst)
    }

    pub fn set_checked(&self, val: bool) {
        self.checked.store(val, Ordering::SeqCst);
    }

    pub fn is_cleaned(&self) -> bool {
        self.cleaned.load(Ordering::SeqCst)
    }

    pub fn set_cleaned(&self) {
        self.cleaned.store(true, Ordering::SeqCst);
    }
}

// impl<'a> fmt::Display for Node<'a> {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         let mut incoming = String::new();
//         let n = self.read().incoming.as_ref().unwrap().lock().len();

//         if n > 0 {
//             incoming.push('[');

//             for (node, rw_edge) in &self.read().incoming.as_ref().unwrap().lock()[0..n - 1] {
//                 incoming.push_str(&format!("{}-{}", node.as_ptr() as usize, rw_edge));
//                 incoming.push_str(", ");
//             }

//             let (node, rw_edge) = &self.read().incoming.as_ref().unwrap().lock()[n - 1].clone();
//             incoming.push_str(&format!("{}-{}]", node.as_ptr() as usize, rw_edge));
//         } else {
//             incoming.push_str("[]");
//         }

//         let mut outgoing = String::new();
//         let m = self.read().incoming.as_ref().unwrap().lock().len();

//         if m > 0 {
//             outgoing.push('[');

//             for (node, rw_edge) in &self.read().outgoing.as_ref().unwrap().lock()[0..m - 1] {
//                 outgoing.push_str(&format!("{}-{}", node.as_ptr() as usize, rw_edge));
//                 outgoing.push_str(", ");
//             }

//             let (node, rw_edge) = self.read().outgoing.as_ref().unwrap().lock()[m - 1].clone();
//             outgoing.push_str(&format!("{}-{}]", node.as_ptr() as usize, rw_edge));
//         } else {6
//             outgoing.push_str("[]");
//         }

//         writeln!(f).unwrap();
//         writeln!(f, "incoming: {}", incoming).unwrap();
//         writeln!(f, "outgoing: {}", outgoing).unwrap();
//         writeln!(f, "committed: {:?}", self.read().committed).unwrap();
//         writeln!(f, "cascading_abort: {:?}", self.read().cascading_abort).unwrap();
//         writeln!(f, "aborted: {:?}", self.read().aborted).unwrap();
//         writeln!(f, "cleaned: {:?}", self.read().cleaned).unwrap();
//         write!(f, "checked: {:?}", self.read().checked).unwrap();

//         Ok(())
//     }
// }
