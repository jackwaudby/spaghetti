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
