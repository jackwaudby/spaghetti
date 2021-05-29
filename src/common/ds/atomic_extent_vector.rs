use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use crossbeam_utils::CachePadded;
use std::sync::atomic::Ordering::{AcqRel, Relaxed};

#[derive(Debug)]
pub struct AtomicExtentVec<T>(Vec<CachePadded<Atomic<T>>>);

impl<T> AtomicExtentVec<T> {
    pub fn reserve(population: usize) -> Self {
        let vec = Vec::with_capacity(population);
        AtomicExtentVec(vec)
    }

    pub fn push(&mut self, val: T) {
        let a = CachePadded::new(Atomic::new(val));
        self.0.push(a);
    }

    pub fn get<'g>(&self, offset: usize, guard: &'g Guard) -> &'g T {
        unsafe { self.0[offset].load(Relaxed, guard).deref() }
    }

    pub fn get_mut<'g>(&self, offset: usize, guard: &'g Guard) -> &'g mut T {
        unsafe { self.0[offset].load(Relaxed, guard).deref_mut() }
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
