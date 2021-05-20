use crate::workloads::PrimaryKey;

#[derive(Debug)]
pub struct TransactionInformation {
    operations: Option<Vec<Operation>>,
}

#[derive(Debug, Clone)]
pub struct Operation {
    pub op_type: OperationType,
    pub key: PrimaryKey,
    pub index: usize,
}

#[derive(Debug, Clone)]
pub enum OperationType {
    Read,
    Write,
}

impl TransactionInformation {
    pub fn new() -> Self {
        TransactionInformation {
            operations: Some(Vec::with_capacity(8)),
        }
    }

    pub fn add(&mut self, op_type: OperationType, key: PrimaryKey, index: usize) {
        self.operations
            .as_mut()
            .unwrap()
            .push(Operation::new(op_type, key, index));
    }

    pub fn get(&mut self) -> Vec<Operation> {
        self.operations.take().unwrap()
    }
}

impl Default for TransactionInformation {
    fn default() -> Self {
        Self::new()
    }
}

impl Operation {
    pub fn new(op_type: OperationType, key: PrimaryKey, index: usize) -> Self {
        Operation {
            op_type,
            key,
            index,
        }
    }
}
