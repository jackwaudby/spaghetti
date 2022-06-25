use spin::Mutex;

struct WaitManager {
    locks: Vec<Mutex>,
    thread_id: u64,
}

impl WaitManager {
    fn wait(&self) {}

    fn release(&self) {}
}
