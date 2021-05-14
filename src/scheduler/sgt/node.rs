use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

pub type ArcNode = Arc<RwLock<Node>>;
pub type WeakNode = Weak<RwLock<Node>>;

#[derive(Debug)]
pub struct Node {
    incoming: Mutex<Vec<(WeakNode, bool)>>,
    outgoing: Mutex<Vec<(WeakNode, bool)>>,
    committed: AtomicBool,
    cascading_abort: AtomicBool,
    aborted: AtomicBool,
    cleaned: AtomicBool,
    checked: AtomicBool,
}

impl Node {
    pub fn new() -> Self {
        Node {
            incoming: Mutex::new(vec![]),
            outgoing: Mutex::new(vec![]),
            committed: AtomicBool::new(false),
            cascading_abort: AtomicBool::new(false),
            aborted: AtomicBool::new(false),
            cleaned: AtomicBool::new(false),
            checked: AtomicBool::new(false),
        }
    }

    pub fn incoming_edge_exists(&self, from_node: WeakNode) -> bool {
        let guard = self.incoming.lock().unwrap();
        let exists = guard.iter().any(|(edge, _)| from_node.ptr_eq(&edge));
        drop(guard);
        exists
    }

    pub fn is_incoming(&self) -> bool {
        let guard = self.incoming.lock().unwrap();
        let emp = !guard.is_empty();

        drop(guard);
        emp
    }

    pub fn insert_incoming(&self, from_node: WeakNode, rw_edge: bool) {
        let mut guard = self.incoming.lock().unwrap();
        guard.push((from_node, rw_edge));
        drop(guard);
    }

    pub fn remove_incoming(&self, from_node: WeakNode) {
        let mut guard = self.incoming.lock().unwrap();
        guard.retain(|(edge, _)| !from_node.ptr_eq(&edge));

        drop(guard);
    }

    pub fn insert_outgoing(&self, to_node: WeakNode, rw_edge: bool) {
        let mut guard = self.outgoing.lock().unwrap();
        guard.push((to_node, rw_edge));
        drop(guard);
    }

    pub fn clear_incoming(&self) {
        let mut guard = self.incoming.lock().unwrap();
        guard.clear();
        drop(guard);
    }

    pub fn clear_outgoing(&self) {
        let mut guard = self.outgoing.lock().unwrap();
        guard.clear();
        drop(guard);
    }

    pub fn get_outgoing(&self) -> Vec<(WeakNode, bool)> {
        let guard = self.outgoing.lock().unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
