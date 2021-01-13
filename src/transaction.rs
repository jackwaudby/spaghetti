//! This trait is implemented on workloads transaction profiles.

use crate::frame::Frame;

use std::any::Any;

// All transactions should be able to be converted into a `Frame`.
pub trait Transaction {
    fn into_frame(&self) -> Frame;
    fn as_any(&self) -> &dyn Any;
}
