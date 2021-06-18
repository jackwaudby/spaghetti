use crate::scheduler::owh::transaction;
use crate::scheduler::sgt::node;

use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Access {
    Read(TransactionId),
    Write(TransactionId),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionId {
    NoConcurrencyControl,
    SerializationGraph(usize),
    OptimisticWaitHit(usize),
}

impl fmt::Display for Access {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Access::*;
        match &self {
            Read(id) => write!(f, "r({})", id),
            Write(id) => write!(f, "w({})", id),
        }
    }
}

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TransactionId::*;

        match &self {
            NoConcurrencyControl => write!(f, "no id"),
            SerializationGraph(txn) => write!(f, "{}", txn),
            OptimisticWaitHit(txn) => write!(f, "{}", txn),
        }
    }
}

// impl PartialEq for TransactionId {
//     fn eq(&self, other: &Self) -> bool {
//         match (self, other) {
//             (
//                 &TransactionId::SerializationGraph(ref wn1),
//                 &TransactionId::SerializationGraph(ref wn2),
//             ) => wn1 == wn2,
//             (
//                 &TransactionId::OptimisticWaitHit(ref wn1),
//                 &TransactionId::OptimisticWaitHit(ref wn2),
//             ) => wn1 == wn2,

//             (&TransactionId::NoConcurrencyControl, &TransactionId::NoConcurrencyControl) => true,
//             _ => false,
//         }
//     }
// }

pub fn access_eq(a: &Access, b: &Access) -> bool {
    matches!(
        (a, b),
        (&Access::Read(..), &Access::Read(..)) | (&Access::Write(..), &Access::Write(..))
    )
}
