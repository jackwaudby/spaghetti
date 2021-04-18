use test_env_log::test;

const PROTOCOL: &str = "sgt";

mod common;

#[test]
fn acid_sgt_g1a() {
    common::g1a(PROTOCOL);
}

#[test]
#[ignore]
fn acid_sgt_g1c() {
    common::g1c(PROTOCOL);
}

#[test]
fn acid_sgt_imp() {
    common::imp(PROTOCOL);
}

#[test]
fn acid_sgt_lu() {
    common::lu(PROTOCOL);
}
