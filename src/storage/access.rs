use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Access {
    Read(TransactionId),
    Write(TransactionId),
}

#[derive(Debug, Clone)]
pub enum TransactionId {
    NoConcurrencyControl,
    SerializationGraph(usize),
    OptimisticWaitHit(usize, usize),
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
            SerializationGraph(node) => write!(f, "{}", node),
            OptimisticWaitHit(thread_id, seq_num) => write!(f, "({}-{})", thread_id, seq_num),
        }
    }
}

impl PartialEq for TransactionId {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                &TransactionId::SerializationGraph(ref wn1),
                &TransactionId::SerializationGraph(ref wn2),
            ) => wn1 == wn2,

            (&TransactionId::OptimisticWaitHit(a, b), &TransactionId::OptimisticWaitHit(c, d)) => {
                (a == c) && (b == d)
            }
            (&TransactionId::NoConcurrencyControl, &TransactionId::NoConcurrencyControl) => true,
            _ => false,
        }
    }
}

pub fn access_eq(a: &Access, b: &Access) -> bool {
    matches!(
        (a, b),
        (&Access::Read(..), &Access::Read(..)) | (&Access::Write(..), &Access::Write(..))
    )
}
