use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Access {
    Read(TransactionId),
    Write(TransactionId),
}

#[derive(Serialize, Deserialize, Eq, Hash, PartialEq, Debug, Clone, Copy)]
pub enum TransactionId {
    NoConcurrencyControl,
    SerializationGraph(usize),
    OptimisticWaitHit(usize),
    Attendez(usize),
    WaitHit(u64),
    TwoPhaseLocking((u64, u64)),
}

impl TransactionId {
    pub fn extract(&self) -> u64 {
        if let TransactionId::SerializationGraph(id) = &self {
            *id as u64
        } else {
            0
        }
    }

    pub fn extract_pair(&self) -> (u64, u64) {
        if let TransactionId::TwoPhaseLocking(id) = &self {
            *id
        } else {
            panic!("not 2pl");
        }
    }
}

impl fmt::Display for Access {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Access::*;
        match &self {
            Read(id) => write!(f, "r-{}", id),
            Write(id) => write!(f, "w-{}", id),
        }
    }
}

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use TransactionId::*;

        match &self {
            NoConcurrencyControl => write!(f, "no id"),
            SerializationGraph(ref_id) => {
                write!(f, "ref id:{}", ref_id)
            }
            Attendez(txn) => write!(f, "{}", txn),
            OptimisticWaitHit(txn) => write!(f, "{}", txn),
            WaitHit(txn) => write!(f, "{}", txn),
            TwoPhaseLocking(txn) => write!(f, "{:?}", txn),
        }
    }
}

pub fn access_eq(a: &Access, b: &Access) -> bool {
    matches!(
        (a, b),
        (&Access::Read(..), &Access::Read(..)) | (&Access::Write(..), &Access::Write(..))
    )
}
