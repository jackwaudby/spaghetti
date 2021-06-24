use test_env_log::test;

mod common;

// SGT
#[test]
#[ignore]
fn acid_sgt_g0() {
    common::g0("sgt");
}

#[test]
fn acid_sgt_g1a() {
    common::g1a("sgt");
}

#[test]
fn acid_sgt_g1c() {
    common::g1c("sgt");
}

#[test]
fn acid_sgt_imp() {
    common::imp("sgt");
}

#[test]
fn acid_sgt_otv() {
    common::otv("sgt");
}

#[test]
fn acid_sgt_fr() {
    common::fr("sgt");
}

#[test]
fn acid_sgt_lu() {
    common::lu("sgt");
}

#[test]
fn acid_sgt_g2item() {
    common::g2item("sgt");
}

// WH
#[test]
#[ignore]
fn acid_wh_g0() {
    common::g0("wh");
}

#[test]
fn acid_wh_g1a() {
    common::g1a("wh");
}

#[test]
fn acid_wh_g1c() {
    common::g1c("wh");
}

#[test]
fn acid_wh_imp() {
    common::imp("wh");
}

#[test]
fn acid_wh_otv() {
    common::otv("wh");
}

#[test]
fn acid_wh_fr() {
    common::fr("wh");
}

#[test]
fn acid_wh_lu() {
    common::lu("wh");
}

#[test]
fn acid_wh_g2item() {
    common::g2item("wh");
}

// OWH
#[test]
#[ignore]
fn acid_owh_g0() {
    common::g0("owh");
}

#[test]
fn acid_owh_g1a() {
    common::g1a("owh");
}

#[test]
fn acid_owh_g1c() {
    common::g1c("owh");
}

#[test]
fn acid_owh_imp() {
    common::imp("owh");
}

#[test]
fn acid_owh_otv() {
    common::otv("owh");
}

#[test]
fn acid_owh_fr() {
    common::fr("owh");
}

#[test]
fn acid_owh_lu() {
    common::lu("owh");
}

#[test]
fn acid_owh_g2item() {
    common::g2item("owh");
}

// NOCC
#[test]
#[ignore]
fn acid_nocc_g0() {
    common::g0("nocc");
}

#[test]
#[should_panic]
fn acid_nocc_g1a() {
    common::g1a("nocc");
}

#[test]
#[should_panic]
fn acid_nocc_g1c() {
    common::g1c("nocc");
}

#[test]
#[should_panic]
fn acid_nocc_imp() {
    common::imp("nocc");
}

#[test]
#[should_panic]
fn acid_nocc_otv() {
    common::otv("nocc");
}

#[test]
#[should_panic]
fn acid_nocc_fr() {
    common::fr("nocc");
}

#[test]
#[should_panic]
fn acid_nocc_lu() {
    common::lu("nocc");
}

#[test]
#[should_panic]
fn acid_nocc_g2item() {
    common::g2item("nocc");
}
