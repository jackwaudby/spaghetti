use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct LatencyBreakdown {
    total: u128,
    write: u128,
    write_cnt: u32,
    read: u128,
    read_cnt: u32,
    commit: u128,
    abort: u128,
}

impl LatencyBreakdown {
    pub fn new() -> Self {
        Self {
            total: 0,
            write: 0,
            write_cnt: 0,
            read: 0,
            read_cnt: 0,
            commit: 0,
            abort: 0,
        }
    }

    pub fn set_total(&mut self, total: u128) {
        self.total = total;
    }

    pub fn get_total(&self) -> u128 {
        self.total
    }

    pub fn add_read(&mut self, dur: u128) {
        self.read += dur;
        self.read_cnt += 1;
    }

    pub fn add_write(&mut self, dur: u128) {
        self.write += dur;
        self.write_cnt += 1;
    }

    pub fn get_write(&self) -> u128 {
        self.write
    }

    pub fn get_write_cnt(&self) -> u32 {
        self.write_cnt
    }

    pub fn add_commit(&mut self, dur: u128) {
        self.commit += dur;
    }

    pub fn get_commit(&self) -> u128 {
        self.commit
    }

    pub fn add_abort(&mut self, dur: u128) {
        self.abort += dur;
    }

    pub fn merge(&mut self, other: &LatencyBreakdown) {
        self.total += other.total;
        self.write += other.write;
        self.write_cnt += other.write_cnt;
        self.read += other.read;
        self.read_cnt += other.read_cnt;
        self.commit += other.commit;
        self.abort += other.abort;
    }
}
