use criterion::{criterion_group, criterion_main, Criterion};
use spaghetti::common::statistics::GlobalStatistics;
use spaghetti::gpc::helper;

use config::Config;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Instant;

fn my_func(cores: usize) {
    let mut c = Config::default();
    c.merge(config::File::with_name("./tests/Test-basic-sgt.toml"))
        .unwrap();

    let config = Arc::new(c);

    let mut global_stats = GlobalStatistics::new(Arc::clone(&config)); // init stats

    let dg_start = Instant::now(); // init database
    let workload = helper::init_database(Arc::clone(&config));
    let dg_end = dg_start.elapsed();
    global_stats.set_data_generation(dg_end);

    let scheduler = helper::init_scheduler(Arc::clone(&workload), cores); // init scheduler
    let (tx, _) = mpsc::channel(); // channel to send statistics

    global_stats.start();
    helper::run(
        cores,
        Arc::clone(&scheduler),
        Arc::clone(&config),
        tx.clone(),
    );
    global_stats.end();
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("basic-sgt");
    group.sample_size(10);
    group.bench_function("1-core", |b| b.iter(|| my_func(1)));
    group.bench_function("4-cores", |b| b.iter(|| my_func(4)));
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
