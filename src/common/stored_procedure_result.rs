use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::common::error::NonFatalError;
use crate::common::isolation_level::IsolationLevel;
use crate::common::message::{Outcome, Success};
use crate::common::stats_bucket::StatsBucket;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct StoredProcedureResult {
    isolation: IsolationLevel,
    outcome: Outcome,
    internal_id: u64,
    problem_transactions: HashSet<usize>,
}

impl StoredProcedureResult {
    pub fn from_error(
        error: NonFatalError,
        isolation: IsolationLevel,
        meta: &mut StatsBucket,
    ) -> StoredProcedureResult {
        let outcome = Outcome::Aborted(error);
        let internal_id = meta.get_transaction_id().extract();
        let problem_transactions = meta.get_problem_transactions();

        StoredProcedureResult {
            isolation,
            outcome,
            internal_id,
            problem_transactions,
        }
    }

    pub fn from_success(
        success: Success,
        isolation: IsolationLevel,
        meta: &mut StatsBucket,
    ) -> StoredProcedureResult {
        let outcome = Outcome::Committed(success);
        let internal_id = meta.get_transaction_id().extract();

        StoredProcedureResult {
            isolation,
            outcome,
            internal_id,
            problem_transactions: HashSet::new(),
        }
    }

    pub fn get_internal_id(&self) -> u64 {
        self.internal_id
    }

    pub fn get_isolation_level(&self) -> &IsolationLevel {
        &self.isolation
    }

    pub fn get_outcome(&self) -> &Outcome {
        &self.outcome
    }

    pub fn get_problem_transactions(&self) -> HashSet<usize> {
        self.problem_transactions.clone()
    }
}
