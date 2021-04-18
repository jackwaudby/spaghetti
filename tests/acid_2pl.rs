const PROTOCOL: &str = "2pl";

mod common;

#[test]
fn acid_2pl_g1a() {
    common::g1a(PROTOCOL);
}

#[test]
fn acid_2pl_g1c() {
    common::g1c(PROTOCOL);
}
