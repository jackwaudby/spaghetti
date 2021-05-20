use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::thread;
use std::time::Duration;

const MAX_SLOTS: u32 = 5;

pub struct WaitManager {
    retries: u32,
    slots: Vec<u64>,
    rng: StdRng,
}

impl WaitManager {
    pub fn new() -> Self {
        let rng = SeedableRng::from_entropy();

        let size = usize::pow(2, MAX_SLOTS);

        let mut slots = Vec::with_capacity(size);

        for i in 0..size {
            slots.push(1 * i as u64);
        }

        WaitManager {
            retries: 0,
            slots,
            rng,
        }
    }

    pub fn wait(&mut self) {
        if self.retries < MAX_SLOTS {
            self.retries += 1;
        }
        let n = (2 ^ self.retries) as usize;
        let ind = self.rng.gen_range(0..n);
        let wait_time = self.slots[ind];
        thread::sleep(Duration::from_micros(wait_time));
    }

    pub fn reset(&mut self) {
        self.retries = 0;
    }
}
