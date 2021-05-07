use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use spaghetti::gpc::helper;

use config::Config;
use std::sync::mpsc;
use std::sync::Arc;

fn gpc(cores: usize) {
    let mut c = Config::default();
    c.merge(config::File::with_name("./tests/Test-basic-sgt.toml"))
        .unwrap();
    let config = Arc::new(c);
    let workload = helper::init_database(Arc::clone(&config));
    let scheduler = helper::init_scheduler(Arc::clone(&workload), cores);
    let (tx, _) = mpsc::channel();

    helper::run(
        cores,
        Arc::clone(&scheduler),
        Arc::clone(&config),
        tx.clone(),
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("basic-sgt");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("1-core", |b| b.iter(|| gpc(1)));
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
