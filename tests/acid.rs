use test_env_log::test;

mod common;

#[test]
fn acid_sgt_g1a() {
    common::g1a("sgt");
}

#[test]
fn acid_wh_g1a() {
    common::g1a("wh");
}

#[test]
fn acid_owh_g1a() {
    common::g1a("owh");
}

#[test]
#[should_panic]
fn acid_nocc_g1a() {
    common::g1a("nocc");
}
