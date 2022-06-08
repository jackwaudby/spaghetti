#[derive(Debug, Clone, Copy)]
pub struct ValueId {
    table_id: usize,
    column_id: usize,
    offset: usize,
}

impl ValueId {
    pub fn new(table_id: usize, column_id: usize, offset: usize) -> Self {
        Self {
            table_id,
            column_id,
            offset,
        }
    }

    pub fn get_table_id(&self) -> usize {
        self.table_id
    }

    pub fn get_column_id(&self) -> usize {
        self.column_id
    }

    pub fn get_offset(&self) -> usize {
        self.offset
    }
}
