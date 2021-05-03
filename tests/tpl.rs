use config::Config;
use std::sync::Arc;
use test_env_log::test;
use tokio::time::{sleep, Duration};

const PROTOCOL: &str = "2pl";

mod common;

#[test]
#[ignore]
fn acid_2pl_g0() {
    common::g0(PROTOCOL);
}

#[test]
fn acid_2pl_g1a() {
    common::g1a(PROTOCOL);
}

#[test]
#[ignore]
fn acid_2pl_g1c() {
    common::g1c(PROTOCOL);
}

#[test]
fn acid_2pl_imp() {
    common::imp(PROTOCOL);
}

#[test]
fn acid_2pl_otv() {
    common::otv(PROTOCOL);
}

#[test]
fn acid_2pl_fr() {
    common::fr(PROTOCOL);
}

#[test]
fn acid_2pl_lu() {
    common::lu(PROTOCOL);
}

#[test]
#[ignore]
fn acid_2pl_g2item() {
    common::g2item(PROTOCOL);
}

fn setup_config() -> Arc<Config> {
    let mut c = Config::default();
    c.merge(config::File::with_name("./tests/Test-tpl.toml"))
        .unwrap();
    Arc::new(c)
}

#[test(tokio::test)]
#[ignore]
async fn tpl_integration_test() {
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
}
