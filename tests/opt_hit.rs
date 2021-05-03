use config::Config;
use std::sync::Arc;
use test_env_log::test;
use tokio::time::{sleep, Duration};
use tracing::info;

const PROTOCOL: &str = "opt-hit";

mod common;

#[test]
fn acid_opt_hit_g0() {
    common::g0(PROTOCOL);
}

#[test]
fn acid_opt_hit_g1a() {
    common::g1a(PROTOCOL);
}

#[test]
fn acid_opt_hit_g1c() {
    common::g1c(PROTOCOL);
}

#[test]
fn acid_opt_hit_imp() {
    common::imp(PROTOCOL);
}

#[test]
fn acid_opt_hit_otv() {
    common::otv(PROTOCOL);
}

#[test]
fn acid_opt_hit_fr() {
    common::fr(PROTOCOL);
}

#[test]
fn acid_opt_hit_lu() {
    common::lu(PROTOCOL);
}

#[test]
fn acid_opt_hit_g2item() {
    common::g2item(PROTOCOL);
}

fn setup_config() -> Arc<Config> {
    let mut c = Config::default();
    c.merge(config::File::with_name("./tests/Test-opt-hit.toml"))
        .unwrap();
    Arc::new(c)
}

#[test(tokio::test)]
#[ignore]
async fn opt_hit_integration_test() {
    info!("Starting opt-hit-list integration test...");
    let config = setup_config();
    let n_clients = config.get_int("clients").unwrap();
    let c = Arc::clone(&config);
    let server = tokio::spawn(async move {
        assert_eq!((), spaghetti::server::run(c).await.unwrap());
    });

    sleep(Duration::from_millis(1000)).await;

    let mut clients = vec![];
    for _ in 0..n_clients {
        let c = Arc::clone(&config);
        let client = tokio::spawn(async move {
            assert_eq!((), spaghetti::client::run(c).await.unwrap());
        });
        clients.push(client);
    }

    for client in clients {
        assert_eq!(client.await.unwrap(), ());
    }

    assert_eq!(server.await.unwrap(), ());
    info!("Finished opt-hit-list integration test");
}
