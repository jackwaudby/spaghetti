use crate::workloads::PrimaryKey;

#[derive(Debug)]
pub struct TransactionInformation {
    operations: Option<Vec<Operation>>,
}

#[derive(Debug, Clone)]
pub struct Operation {
    pub op_type: OperationType,
    pub key: PrimaryKey,
    pub index: String,
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

    pub fn add(&mut self, op_type: OperationType, key: PrimaryKey, index: String) {
        self.operations
            .as_mut()
            .unwrap()
            .push(Operation::new(op_type, key, index));
    }

    pub fn get(&mut self) -> Vec<Operation> {
        self.operations.take().unwrap()
    }
}

impl Operation {
    pub fn new(op_type: OperationType, key: PrimaryKey, index: String) -> Self {
        Operation {
            op_type,
            key,
            index,
        }
    }
}
