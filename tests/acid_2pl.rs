use test_env_log::test;

const PROTOCOL: &str = "2pl";

mod common;

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
fn acid_2pl_lu() {
    common::lu(PROTOCOL);
}
