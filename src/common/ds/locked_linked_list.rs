use spin::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct LockedLinkedList<T> {
    id: AtomicU64,
    dat: Arc<Mutex<Vec<Node<T>>>>,
}

#[derive(Debug)]
struct Node<T> {
    id: u64,
    val: T,
}

impl<T> Node<T> {
    fn get_id(&self) -> u64 {
        self.id
    }
}

impl<T> LockedLinkedList<T> {
    pub fn new() -> Self {
        Self {
            id: AtomicU64::new(0),
            dat: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn push_front(&self, val: T) -> u64 {
        let mut g = self.dat.lock();
        let id = self.id.fetch_add(1, Ordering::SeqCst); // get node id
        g.push(Node { id, val });
        drop(g);
        id
    }

    pub fn erase(&self, id: u64) {
        let mut g = self.dat.lock();
        g.retain(|n| n.get_id() != id);
        drop(g);
    }
}
