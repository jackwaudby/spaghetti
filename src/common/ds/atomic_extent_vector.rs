use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};

#[derive(Debug)]
pub struct AtomicExtentVec<T>(Vec<Atomic<T>>);

impl<T> AtomicExtentVec<T> {
    pub fn reserve(population: usize) -> Self {
        let vec = Vec::with_capacity(population);
        AtomicExtentVec(vec)
    }

    pub fn push(&mut self, val: T) {
        let a = Atomic::new(val);
        self.0.push(a);
    }

    pub fn get<'g>(&self, offset: usize, guard: &'g Guard) -> Option<&'g T> {
        unsafe { self.0[offset].load(Acquire, guard).as_ref() }
    }

    pub fn get_mut<'g>(&self, offset: usize, guard: &'g Guard) -> &'g mut T {
        unsafe { self.0[offset].load(Acquire, guard).deref_mut() }
    }

    pub fn replace<'g>(&self, offset: usize, val: T, guard: &'g Guard) {
        let val = Owned::new(val);
        self.0[offset].swap(val, AcqRel, guard);
    }
}

impl<T> Drop for AtomicExtentVec<T> {
    fn drop(&mut self) {
        // Safety: unprotected access to `Atomic`s is ok as no concurrent thread can access the list.
        unsafe {
            for entry in &self.0 {
                let next = entry.load(Relaxed, epoch::unprotected());
                let o = next.into_owned(); // take ownership
                drop(o); // deallocate it
            }
        }
    }
}
