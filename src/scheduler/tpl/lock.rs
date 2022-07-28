use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct Lock {
    write: Option<(u64, u64)>,
    read: HashSet<(u64, u64)>,
}

impl Lock {
    pub fn new() -> Self {
        Self {
            write: None,
            read: HashSet::new(),
        }
    }

    pub fn get_write_info(&self) -> &Option<(u64, u64)> {
        &self.write
    }

    pub fn get_read_info(&self) -> &HashSet<(u64, u64)> {
        &self.read
    }

    pub fn get_mut_read_info(&mut self) -> &mut HashSet<(u64, u64)> {
        &mut self.read
    }

    pub fn set_write_info(&mut self, new: Option<(u64, u64)>) {
        self.write = new;
    }

    pub fn other_transaction_holds_write_lock(&self, this_id: (u64, u64)) -> bool {
        match self.write {
            Some(held_by_id) => held_by_id != this_id,
            None => false,
        }
    }

    pub fn multiple_read_locks_held(&self) -> bool {
        self.read.len() > 1
    }
}
