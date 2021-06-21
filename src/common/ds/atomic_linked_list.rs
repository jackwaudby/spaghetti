use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use spin::Mutex;

use std::fmt;
use std::fmt::Debug;
use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed, SeqCst};

#[derive(Debug)]
pub struct AtomicLinkedList<T> {
    id: AtomicU64,
    head: Atomic<Node<T>>,
    lock: Mutex<u64>,
}

#[derive(Debug)]
struct Node<T> {
    id: ManuallyDrop<u64>,
    data: ManuallyDrop<T>,
    next: ManuallyDrop<Atomic<Node<T>>>,
}

impl<T> AtomicLinkedList<T> {
    /// Create an empty list.
    pub fn new() -> Self {
        AtomicLinkedList {
            id: AtomicU64::new(0),
            head: Atomic::null(),
            lock: Mutex::new(0),
        }
    }

    /// Push an element to the head of the list. Returns (all-time) position in the list.
    pub fn push_front<'g>(&self, t: T, guard: &'g Guard) -> u64 {
        let id = self.id.fetch_add(1, SeqCst); // get node id

        let mut new = Owned::new(Node {
            id: ManuallyDrop::new(id),
            data: ManuallyDrop::new(t),
            next: ManuallyDrop::new(Atomic::null()),
        }); // create node

        loop {
            let head = self.head.load(Relaxed, guard); // snapshot current

            new.next.store(head, Relaxed); // update next pointer

            // if snapshot is valid then link new node
            match self
                .head
                .compare_exchange(head, new, Relaxed, Relaxed, guard)
            {
                Ok(_) => {
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
                                guard.defer_destroy(head_snapshot);
                                drop(guard);
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

    pub fn erase<'g>(&self, id: u64, guard: &'g Guard) {
        let lg = self.lock.lock(); // 1 erase at a time
        let mut left = Shared::null(); // Shared
        let mut current;
        loop {
            current = self.head.load(Acquire, guard); // Shared
                                                      // traverse until current is node to be removed
            while let Some(node) = unsafe { current.as_ref() } {
                if *node.id == id {
                    break;
                }
                left = current;
                current = node.next.load(Acquire, guard); // Shared
            }

            let right = match unsafe { current.as_ref() } {
                Some(node) => node.next.load(Acquire, guard),
                None => {
                    drop(lg);
                    //        drop(guard);
                    return;
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

        drop(lg);
        unsafe {
            guard.defer_unchecked(move || {
                let mut o = current.into_owned();
                ManuallyDrop::drop(&mut o.data);
                ManuallyDrop::drop(&mut o.id);
                ManuallyDrop::drop(&mut o.next);

                drop(o);
            });
        }
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
    type Item = (&'g u64, &'g T);

    fn next(&mut self) -> Option<Self::Item> {
        match self.next {
            Some(node) => {
                self.next = unsafe { node.next.load(Acquire, self.guard).as_ref() };

                Some((&*node.id, &*node.data))
            }
            None => None,
        }
    }
}

impl<T: Debug> fmt::Display for AtomicLinkedList<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let guard = &epoch::pin();

        let it = self.iter(guard);

        let mut res = String::new();
        res.push_str(&format!("["));

        for access in it {
            res.push_str(&format!("{:?}", access));
            res.push_str(", ");
        }

        res.pop(); // remove trailing ', '
        res.pop();
        res.push_str(&format!("]"));

        writeln!(f, "{}", res).unwrap();
        Ok(())
    }
}
