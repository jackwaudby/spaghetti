use crate::common::message::{Outcome, Transaction};
use crate::workloads::acid::AcidTransaction;
use crate::workloads::dummy::DummyTransaction;
use crate::workloads::smallbank::SmallBankTransaction;
use crate::workloads::tatp::TatpTransaction;
use crate::workloads::ycsb::YcsbTransaction;

use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransactionBreakdown {
    name: String,
    transactions: Vec<TransactionMetrics>,
}

impl TransactionBreakdown {
    pub fn new(workload: &str) -> Self {
        match workload {
            "smallbank" => {
                let name = workload.to_string();
                let mut transactions = vec![];
                for transaction in SmallBankTransaction::iter() {
                    let metrics = TransactionMetrics::new(Transaction::SmallBank(transaction));
                    transactions.push(metrics);
                }
                TransactionBreakdown { name, transactions }
            }

            "acid" => {
                let name = workload.to_string();
                let mut transactions = vec![];
                for transaction in AcidTransaction::iter() {
                    let metrics = TransactionMetrics::new(Transaction::Acid(transaction));
                    transactions.push(metrics);
                }
                TransactionBreakdown { name, transactions }
            }

            "dummy" => {
                let name = workload.to_string();
                let mut transactions = vec![];
                for transaction in DummyTransaction::iter() {
                    let metrics = TransactionMetrics::new(Transaction::Dummy(transaction));
                    transactions.push(metrics);
                }
                TransactionBreakdown { name, transactions }
            }

            "tatp" => {
                let name = workload.to_string();
                let mut transactions = vec![];
                for transaction in TatpTransaction::iter() {
                    let metrics = TransactionMetrics::new(Transaction::Tatp(transaction));
                    transactions.push(metrics);
                }
                TransactionBreakdown { name, transactions }
            }

            "ycsb" => {
                let name = workload.to_string();
                let mut transactions = vec![];
                for transaction in YcsbTransaction::iter() {
                    let metrics = TransactionMetrics::new(Transaction::Ycsb(transaction));
                    transactions.push(metrics);
                }
                TransactionBreakdown { name, transactions }
            }

            _ => panic!("unknown workload: {}", workload),
        }
    }

    pub fn record(&mut self, transaction: &Transaction, outcome: &Outcome) {
        let ind = self
            .transactions
            .iter()
            .position(|x| &x.transaction == transaction)
            .unwrap();

        match outcome {
            Outcome::Committed { .. } => self.transactions[ind].inc_committed(),
            Outcome::Aborted { .. } => self.transactions[ind].inc_aborted(),
        }
    }

    pub fn merge(&mut self, other: TransactionBreakdown) {
        assert!(self.name == other.name);

        for holder in other.transactions {
            let ind = self
                .transactions
                .iter()
                .position(|x| x.transaction == holder.transaction)
                .unwrap();
            self.transactions[ind].merge(holder);
        }
    }

    pub fn get_transactions(&mut self) -> &mut Vec<TransactionMetrics> {
        &mut self.transactions
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransactionMetrics {
    transaction: Transaction,
    completed: u32,
    committed: u32,
    aborted: u32, // internal and external aborts
}

impl TransactionMetrics {
    pub fn new(transaction: Transaction) -> Self {
        TransactionMetrics {
            transaction,
            completed: 0,
            committed: 0,
            aborted: 0,
        }
    }

    pub fn inc_committed(&mut self) {
        self.committed += 1;
        self.completed += 1;
    }

    pub fn inc_aborted(&mut self) {
        self.completed += 1;
        self.aborted += 1;
    }

    pub fn merge(&mut self, other: TransactionMetrics) {
        assert!(self.transaction == other.transaction);

        self.completed += other.completed;
        self.committed += other.committed;
        self.aborted += other.aborted;
    }

    pub fn get_completed(&self) -> u32 {
        self.completed
    }

    pub fn get_committed(&self) -> u32 {
        self.committed
    }

    pub fn get_aborted(&self) -> u32 {
        self.aborted
    }
}
