use config::Config;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use crossbeam_utils::thread;
use std::sync::mpsc;
use std::time::Instant;

use spaghetti::common::statistics::GlobalStatistics;
use spaghetti::common::utils;
use spaghetti::scheduler::Scheduler;
use spaghetti::storage::Database;

fn setup() -> (Scheduler<'static>, Database) {
    let mut config = utils::init_config("./Settings.toml");
    config.set("cores", 1).unwrap();
    config.set("protocol", "nocc").unwrap();

    let database: Database = Database::new(&config).unwrap();
    let scheduler: Scheduler = Scheduler::new(&config).unwrap();

    (scheduler, database)
}

fn run(data: (Scheduler, Database)) {
    let mut config = utils::init_config("./Settings.toml");
    config.set("cores", 1).unwrap();
    config.set("protocol", "nocc").unwrap();

    let (scheduler, database) = data;

    let (tx, rx) = mpsc::channel();

    let core_ids = core_affinity::get_core_ids().unwrap();

    thread::scope(|s| {
        let scheduler = &scheduler;
        let database = &database;
        let config = &config;

        for (thread_id, core_id) in core_ids.iter().enumerate() {
            let txc = tx.clone();

            s.builder()
                .name(thread_id.to_string())
                .spawn(move |_| {
                    core_affinity::set_for_current(*core_id); // pin thread to cpu core
                    utils::run(thread_id, config, scheduler, database, txc, None);
                })
                .unwrap();
        }
    })
    .unwrap();

    // drop(tx);

    // while let Ok(local_stats) = rx.recv() {
    //     global_stats.merge_into(local_stats);
    // }
    // global_stats.write_to_file();
}

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("with_setup", move |b| {
        b.iter_batched(|| setup(), |data| run(data), BatchSize::PerIteration)
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
