use std::slice::Iter;

#[derive(Debug)]
pub struct LocksHeld(Vec<LockId>);

#[derive(Debug)]
pub struct LockId {
    table_id: usize,
    offset: usize,
}

impl LocksHeld {
    pub fn new() -> Self {
        LocksHeld(Vec::new())
    }

    pub fn add(&mut self, table_id: usize, offset: usize) {
        self.0.push(LockId::new(table_id, offset))
    }

    pub fn iter(&self) -> Iter<LockId> {
        self.0.iter()
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }
}

impl LockId {
    fn new(table_id: usize, offset: usize) -> Self {
        Self { table_id, offset }
    }

    pub fn get_id(&self) -> (usize, usize) {
        (self.table_id, self.offset)
    }
}
