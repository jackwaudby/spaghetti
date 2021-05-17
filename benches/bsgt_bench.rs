use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use spaghetti::gpc::helper;

use config::Config;
use std::sync::mpsc;
use std::sync::Arc;

fn gpc(cores: usize) {
    let mut config = Config::default();
    config
        .merge(config::File::with_name("./tests/Test-sgt.toml"))
        .unwrap();

    let workload = helper::init_database(config.clone());
    let scheduler = helper::init_scheduler(workload, cores);
    let (tx, _) = mpsc::channel(); // channel to send statistics

    helper::run(cores, scheduler, Arc::new(config), tx);
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("sgt");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("1-core", |b| b.iter(|| gpc(1)));
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
