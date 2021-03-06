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
    WaitHit(u64),
    TwoPhaseLocking(u64),
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
            SerializationGraph(txn) => write!(f, "{}", txn),
            OptimisticWaitHit(txn) => write!(f, "{}", txn),
            WaitHit(txn) => write!(f, "{}", txn),
            TwoPhaseLocking(txn) => write!(f, "{}", txn),
        }
    }
}

pub fn access_eq(a: &Access, b: &Access) -> bool {
    matches!(
        (a, b),
        (&Access::Read(..), &Access::Read(..)) | (&Access::Write(..), &Access::Write(..))
    )
}
