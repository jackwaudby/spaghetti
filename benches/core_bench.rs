use criterion::{criterion_group, criterion_main, Criterion};
use crossbeam_utils::thread;
use std::sync::mpsc;
use std::time::Instant;

use spaghetti::common::statistics::GlobalStatistics;
use spaghetti::common::utils;
use spaghetti::scheduler::Scheduler;
use spaghetti::storage::Database;

fn run() {
    let mut config = utils::init_config("./Settings.toml");
    config.set("cores", 1).unwrap();
    config.set("protocol", "nocc").unwrap();

    utils::create_log_dir(&config); // log transaction results

    let mut global_stats = GlobalStatistics::new(&config); // global stats collector
    let (tx, rx) = mpsc::channel();

    let dg_start = Instant::now();
    let database: Database = utils::init_database(&config); // populate database
    let dg_end = dg_start.elapsed();
    global_stats.set_data_generation(dg_end);

    let scheduler: Scheduler = utils::init_scheduler(&config); // create scheduler

    global_stats.start();

    let core_ids = core_affinity::get_core_ids().unwrap(); // always use max cores available

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

    drop(tx);
    global_stats.end();

    while let Ok(local_stats) = rx.recv() {
        global_stats.merge_into(local_stats);
    }
    global_stats.write_to_file();
}

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("nocc", |b| b.iter(|| run()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
