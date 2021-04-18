use test_env_log::test;

const PROTOCOL: &str = "hit";

mod common;

#[test]
fn acid_hit_g1a() {
    common::g1a(PROTOCOL);
}

#[test]
fn acid_hit_g1c() {
    common::g1c(PROTOCOL);
}

#[test]
fn acid_hit_imp() {
    common::imp(PROTOCOL);
}

#[test]
fn acid_hit_lu() {
    common::lu(PROTOCOL);
}
