use test_env_log::test;

const PROTOCOL: &str = "sgt";

mod common;

#[test]
fn acid_sgt_g0() {
    common::g0(PROTOCOL);
}

#[test]
fn acid_sgt_g1a() {
    common::g1a(PROTOCOL);
}

#[test]
fn acid_sgt_g1c() {
    common::g1c(PROTOCOL);
}

#[test]
fn acid_sgt_imp() {
    common::imp(PROTOCOL);
}

#[test]
fn acid_sgt_otv() {
    common::otv(PROTOCOL);
}

#[test]
fn acid_sgt_fr() {
    common::fr(PROTOCOL);
}

#[test]
#[ignore]
fn acid_sgt_lu() {
    common::lu(PROTOCOL);
}

#[test]
#[ignore]
fn acid_sgt_g2item() {
    common::g2item(PROTOCOL);
}
