use cxx::UniquePtr;
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};

#[cxx::bridge]
mod ffi {

    unsafe extern "C++" {
        include!("spaghetti/include/shared_spin_mutex.h");

        type SharedSpinMutex;

        fn new_mutex() -> UniquePtr<SharedSpinMutex>;

        fn lock(self: Pin<&mut SharedSpinMutex>);

        fn unlock(self: Pin<&mut SharedSpinMutex>);

        fn lock_shared(self: Pin<&mut SharedSpinMutex>);

        fn unlock_shared(self: Pin<&mut SharedSpinMutex>);

    }
}

unsafe impl Send for ffi::SharedSpinMutex {}
unsafe impl Sync for ffi::SharedSpinMutex {}

#[derive(Debug)]
pub struct Mutex<T> {
    ffi: UnsafeCell<UniquePtr<ffi::SharedSpinMutex>>,
    data: UnsafeCell<T>,
}

impl<T> Mutex<T> {
    pub fn new(data: T) -> Mutex<T> {
        Mutex {
            ffi: UnsafeCell::new(ffi::new_mutex()),
            data: UnsafeCell::new(data),
        }
    }

    pub fn acquire(&self) -> MutexGuard<'_, T> {
        unsafe {
            self.ffi.get().as_mut().unwrap().pin_mut().lock();
        }
        MutexGuard { mutex: &self }
    }

    pub fn release(&self) {
        unsafe {
            self.ffi.get().as_mut().unwrap().pin_mut().unlock();
        }
    }

    pub fn acquire_shared(&self) -> SharedMutexGuard<'_, T> {
        unsafe {
            self.ffi.get().as_mut().unwrap().pin_mut().lock_shared();
        }

        SharedMutexGuard { mutex: &self }
    }
    pub fn release_shared(&self) {
        unsafe {
            self.ffi.get().as_mut().unwrap().pin_mut().unlock_shared();
        }
    }
}

#[derive(Debug)]
pub struct MutexGuard<'a, T> {
    mutex: &'a Mutex<T>,
}

impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.mutex.data.get() }
    }
}

impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.mutex.data.get() }
    }
}

impl<T> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        self.mutex.release()
    }
}

#[derive(Debug)]
pub struct SharedMutexGuard<'a, T> {
    mutex: &'a Mutex<T>,
}

impl<T> Deref for SharedMutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.mutex.data.get() }
    }
}

impl<T> DerefMut for SharedMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.mutex.data.get() }
    }
}

impl<T> Drop for SharedMutexGuard<'_, T> {
    fn drop(&mut self) {
        self.mutex.release_shared()
    }
}

unsafe impl<T> Send for Mutex<T> where T: Send {}
unsafe impl<T> Sync for Mutex<T> where T: Send {}
unsafe impl<T> Send for MutexGuard<'_, T> where T: Send {}
unsafe impl<T> Sync for MutexGuard<'_, T> where T: Send + Sync {}
unsafe impl<T> Send for SharedMutexGuard<'_, T> where T: Send {}
unsafe impl<T> Sync for SharedMutexGuard<'_, T> where T: Send + Sync {}
