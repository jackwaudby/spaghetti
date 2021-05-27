use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};
use std::sync::Mutex;

pub struct AtomicLinkedList<T> {
    id: AtomicU64,
    head: Atomic<Node<T>>,
    size: AtomicU64,
    lock: Mutex<u64>,
}

struct Node<T> {
    id: ManuallyDrop<u64>,
    data: ManuallyDrop<T>,
    next: Atomic<Node<T>>,
}

impl<T> AtomicLinkedList<T> {
    /// Create an empty list.
    pub fn new() -> Self {
        AtomicLinkedList {
            id: AtomicU64::new(0),
            head: Atomic::null(),
            size: AtomicU64::new(0),
            lock: Mutex::new(0),
        }
    }

    /// Push an element to the head of the list. Returns (all-time) position in the list.
    pub fn push_front(&self, t: T) -> u64 {
        let id = self.id.fetch_add(1, SeqCst); // get node id

        let mut new = Owned::new(Node {
            id: ManuallyDrop::new(id),
            data: ManuallyDrop::new(t),
            next: Atomic::null(),
        }); // create node

        let guard = &epoch::pin(); // pin thread

        loop {
            let head = self.head.load(Relaxed, guard); // snapshot current

            new.next.store(head, Relaxed); // update next pointer

            // if snapshot is valid then link new node
            match self
                .head
                .compare_exchange(head, new, Relaxed, Relaxed, guard)
            {
                Ok(_) => {
                    self.size.fetch_add(1, SeqCst); // increment list size
                    return id;
                }
                Err(cse) => {
                    new = cse.new;
                }
            }
        }
    }

    /// Pop off the head of the list.
    pub fn pop(&self) -> Option<T> {
        let guard = &epoch::pin();

        loop {
            let head_snapshot = self.head.load(Acquire, &guard);
            unsafe {
                match head_snapshot.as_ref() {
                    Some(head) => {
                        let next_snapshot = head.next.load(Acquire, guard);

                        match self.head.compare_exchange(
                            head_snapshot,
                            next_snapshot,
                            Acquire,
                            Acquire,
                            guard,
                        ) {
                            Ok(_) => {
                                self.size.fetch_sub(1, SeqCst); // increment list size

                                guard.defer_destroy(head_snapshot);
                                return Some(ptr::read(&*(*head).data));
                            }
                            Err(_) => continue,
                        }
                    }
                    None => return None,
                }
            }
        }
    }

    pub fn size(&self) -> u64 {
        self.size.load(Relaxed)
    }

    pub fn erase(&self, id: u64) -> Option<T> {
        let lg = self.lock.lock().unwrap(); // 1 erase at a time
        let guard = &epoch::pin();

        let mut left = Shared::null(); // Shared
        let mut current;
        loop {
            current = self.head.load(Acquire, &guard); // Shared

            // traverse until current is node to be removed
            while let Some(node) = unsafe { current.as_ref() } {
                if *node.id == id {
                    break;
                }
                left = current;
                current = node.next.load(Acquire, &guard); // Shared
            }

            let right = match unsafe { current.as_ref() } {
                Some(node) => node.next.load(Acquire, &guard),
                None => {
                    drop(lg);
                    return None;
                }
            };

            // take ownership of current

            if !left.is_null() {
                unsafe {
                    match left
                        .as_ref()
                        .unwrap()
                        .next
                        .compare_exchange(current, right, Acquire, Acquire, &guard)
                    {
                        Ok(_) => {
                            break;
                        }
                        // head changed
                        Err(_) => {
                            continue;
                        }
                    }
                };
            } else {
                match self
                    .head
                    .compare_exchange(current, right, Acquire, Acquire, &guard)
                {
                    Ok(_) => {
                        break;
                    }
                    // head changed
                    Err(_) => {
                        continue;
                    }
                }
            }
        }

        self.size.fetch_sub(1, SeqCst); // decrement list size
        unsafe { guard.defer_destroy(current) }; // deallocate
        drop(lg);
        return Some(unsafe { ptr::read(&*(current.as_ref().unwrap()).data) }); // return value
    }
}

impl<T> Drop for AtomicLinkedList<T> {
    fn drop(&mut self) {
        // Safety: unprotected access to `Atomic`s is ok as no concurrent thread can access the list.
        unsafe {
            let mut node = self.head.load(Relaxed, epoch::unprotected()); // Shared

            while let Some(n) = node.as_ref() {
                let next = n.next.load(Relaxed, epoch::unprotected());

                let mut o = node.into_owned(); // take ownership of the node
                ManuallyDrop::drop(&mut o.id); // drop its data
                ManuallyDrop::drop(&mut o.data);
                drop(o); // deallocate it

                node = next;
            }
        }
    }
}

pub struct IntoIter<T>(AtomicLinkedList<T>);

pub struct Iter<'g, T> {
    next: Option<&'g Node<T>>,
    guard: &'g Guard,
}

impl<T> AtomicLinkedList<T> {
    pub fn into_iter(self) -> IntoIter<T> {
        IntoIter(self)
    }

    pub fn iter<'g>(&self, guard: &'g Guard) -> Iter<'g, T> {
        Iter {
            next: unsafe { self.head.load(Acquire, guard).as_ref() },
            guard,
        }
    }
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop()
    }
}

impl<'g, T> Iterator for Iter<'g, T> {
    type Item = &'g T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next {
            Some(node) => {
                self.next = unsafe { node.next.load(Acquire, self.guard).as_ref() };

                Some(&node.data)
            }
            None => None,
        }
    }
}
