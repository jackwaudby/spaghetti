use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

pub type ArcNode = Arc<Node>;
pub type WeakNode = Weak<Node>;

#[derive(Debug)]
pub struct Node(RwLock<Attributes>);

#[derive(Debug)]
pub struct Attributes {
    pub incoming: Option<Mutex<Vec<(WeakNode, bool)>>>,
    outgoing: Option<Mutex<Vec<(WeakNode, bool)>>>,
    committed: AtomicBool,
    cascading_abort: AtomicBool,
    aborted: AtomicBool,
    cleaned: AtomicBool,
    checked: AtomicBool,
}

impl Node {
    pub fn new() -> Self {
        Node(RwLock::new(Attributes::new()))
    }

    pub fn new_with_sets(
        incoming: Mutex<Vec<(WeakNode, bool)>>,
        outgoing: Mutex<Vec<(WeakNode, bool)>>,
    ) -> Self {
        Node(RwLock::new(Attributes::new_with_sets(incoming, outgoing)))
    }

    pub fn read(&self) -> RwLockReadGuard<Attributes> {
        self.0.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<Attributes> {
        self.0.write()
    }
}

impl Default for Node {
    fn default() -> Self {
        Self::new()
    }
}

impl Attributes {
    pub fn new() -> Self {
        Attributes {
            incoming: Some(Mutex::new(vec![])),
            outgoing: Some(Mutex::new(vec![])),
            committed: AtomicBool::new(false),
            cascading_abort: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
            cleaned: AtomicBool::new(false),
            checked: AtomicBool::new(false),
        }
    }

    pub fn new_with_sets(
        incoming: Mutex<Vec<(WeakNode, bool)>>,
        outgoing: Mutex<Vec<(WeakNode, bool)>>,
    ) -> Self {
        Attributes {
            incoming: Some(incoming),
            outgoing: Some(outgoing),
            committed: AtomicBool::new(false),
            cascading_abort: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
            cleaned: AtomicBool::new(false),
            checked: AtomicBool::new(false),
        }
    }

    pub fn incoming_edge_exists(&self, from_node: WeakNode) -> bool {
        let guard = self.incoming.as_ref().unwrap().lock();
        let exists = guard.iter().any(|(edge, _)| from_node.ptr_eq(&edge));
        drop(guard);
        exists
    }

    pub fn is_incoming(&self) -> bool {
        let guard = self.incoming.as_ref().unwrap().lock();
        let emp = !guard.is_empty();

        drop(guard);
        emp
    }

    pub fn insert_incoming(&self, from_node: WeakNode, rw_edge: bool) {
        let mut guard = self.incoming.as_ref().unwrap().lock();
        guard.push((from_node, rw_edge));
        drop(guard);
    }

    pub fn remove_incoming(&self, from_node: WeakNode) {
        let mut guard = self.incoming.as_ref().unwrap().lock();
        guard.retain(|(edge, _)| !from_node.ptr_eq(&edge));

        drop(guard);
    }

    pub fn insert_outgoing(&self, to_node: WeakNode, rw_edge: bool) {
        let mut guard = self.outgoing.as_ref().unwrap().lock();
        guard.push((to_node, rw_edge));
        drop(guard);
    }

    pub fn clear_incoming(&self) {
        let mut guard = self.incoming.as_ref().unwrap().lock();
        guard.clear();
        drop(guard);
    }

    pub fn clear_outgoing(&self) {
        let mut guard = self.outgoing.as_ref().unwrap().lock();
        guard.clear();
        drop(guard);
    }

    pub fn take_incoming(&mut self) -> Mutex<Vec<(WeakNode, bool)>> {
        self.incoming.take().unwrap()
    }

    pub fn take_outgoing(&mut self) -> Mutex<Vec<(WeakNode, bool)>> {
        self.outgoing.take().unwrap()
    }

    pub fn get_outgoing(&self, v1: bool, v2: bool) -> Vec<(WeakNode, bool)> {
        match self.outgoing {
            Some(ref x) => {
                let guard = x.lock();
                let out = guard.clone();
                drop(guard);
                out
            }
            None => {
                let v3 = !self.is_aborted() || !self.is_cascading_abort(); // || !self.is_committed();
                panic!("{:?}; {}; {}; {}", self, v1, v2, v3)
            }
        }

        // let guard = self.outgoing.as_ref().unwrap().lock();
        // let out = guard.clone();

        // drop(guard);
        // out
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

impl Default for Attributes {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut incoming = String::new();
        let n = self.read().incoming.as_ref().unwrap().lock().len();

        if n > 0 {
            incoming.push('[');

            for (node, rw_edge) in &self.read().incoming.as_ref().unwrap().lock()[0..n - 1] {
                incoming.push_str(&format!("{}-{}", node.as_ptr() as usize, rw_edge));
                incoming.push_str(", ");
            }

            let (node, rw_edge) = &self.read().incoming.as_ref().unwrap().lock()[n - 1].clone();
            incoming.push_str(&format!("{}-{}]", node.as_ptr() as usize, rw_edge));
        } else {
            incoming.push_str("[]");
        }

        let mut outgoing = String::new();
        let m = self.read().incoming.as_ref().unwrap().lock().len();

        if m > 0 {
            outgoing.push('[');

            for (node, rw_edge) in &self.read().outgoing.as_ref().unwrap().lock()[0..m - 1] {
                outgoing.push_str(&format!("{}-{}", node.as_ptr() as usize, rw_edge));
                outgoing.push_str(", ");
            }

            let (node, rw_edge) = self.read().outgoing.as_ref().unwrap().lock()[m - 1].clone();
            outgoing.push_str(&format!("{}-{}]", node.as_ptr() as usize, rw_edge));
        } else {
            outgoing.push_str("[]");
        }

        writeln!(f).unwrap();
        writeln!(f, "incoming: {}", incoming).unwrap();
        writeln!(f, "outgoing: {}", outgoing).unwrap();
        writeln!(f, "committed: {:?}", self.read().committed).unwrap();
        writeln!(f, "cascading_abort: {:?}", self.read().cascading_abort).unwrap();
        writeln!(f, "aborted: {:?}", self.read().aborted).unwrap();
        writeln!(f, "cleaned: {:?}", self.read().cleaned).unwrap();
        write!(f, "checked: {:?}", self.read().checked).unwrap();

        Ok(())
    }
}
