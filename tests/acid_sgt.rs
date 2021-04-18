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
