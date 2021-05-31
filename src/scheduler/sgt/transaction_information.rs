#[derive(Debug)]
pub struct TransactionInformation {
    operations: Option<Vec<Operation>>,
}

#[derive(Debug, Clone)]
pub struct Operation {
    pub op_type: OperationType,
    pub table_id: usize,
    pub column_id: usize,
    pub offset: usize,
    pub prv: u64,
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

    pub fn add(
        &mut self,
        op_type: OperationType,
        table_id: usize,
        column_id: usize,
        offset: usize,
        prv: u64,
    ) {
        self.operations
            .as_mut()
            .unwrap()
            .push(Operation::new(op_type, table_id, column_id, offset, prv));
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
    pub fn new(
        op_type: OperationType,
        table_id: usize,
        column_id: usize,
        offset: usize,
        prv: u64,
    ) -> Self {
        Operation {
            op_type,
            table_id,
            column_id,
            offset,
            prv,
        }
    }
}
