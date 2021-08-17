use once_cell::sync::OnceCell;
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt;

struct Logger {
    _guard: WorkerGuard,
}

static INSTANCE: OnceCell<Logger> = OnceCell::new();

fn setup() -> &'static Logger {
    INSTANCE.get_or_init(|| {
        let file_appender = tracing_appender::rolling::hourly("./log/", "debug.log");
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        let subscriber = fmt::Subscriber::builder()
            .with_writer(non_blocking)
            .with_max_level(Level::DEBUG)
            .with_thread_names(true)
            .with_target(false)
            .finish();

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");

        Logger { _guard: guard }
    })
}

mod common;

// SGT
#[test]
fn acid_sgt_g0() {
    common::g0("sgt");
}

#[test]
fn acid_sgt_g1a() {
    setup();
    common::g1a("sgt");
}

#[test]
#[ignore]
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
fn acid_wh_g0() {
    common::g0("wh");
}

#[test]
fn acid_wh_g1a() {
    common::g1a("wh");
}

#[test]
#[ignore]
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
fn acid_owh_g0() {
    common::g0("owh");
}

#[test]
fn acid_owh_g1a() {
    common::g1a("owh");
}

#[test]
#[ignore]
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

// OWHTT
#[test]
fn acid_owhtt_g0() {
    common::g0("owhtt");
}

#[test]
fn acid_owhtt_g1a() {
    common::g1a("owhtt");
}

#[test]
#[ignore]
fn acid_owhtt_g1c() {
    common::g1c("owhtt");
}

#[test]
fn acid_owhtt_imp() {
    common::imp("owhtt");
}

#[test]
fn acid_owhtt_otv() {
    common::otv("owhtt");
}

#[test]
fn acid_owhtt_fr() {
    common::fr("owhtt");
}

#[test]
fn acid_owhtt_lu() {
    common::lu("owhtt");
}

#[test]
fn acid_owhtt_g2item() {
    common::g2item("owhtt");
}

// NOCC
#[test]
#[should_panic]
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
#[ignore]
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

// 2PL
#[test]
fn acid_tpl_g0() {
    setup();
    common::g0("tpl");
}

#[test]
fn acid_tpl_g1a() {
    setup();
    common::g1a("tpl");
}

#[test]
fn acid_tpl_g1c() {
    common::g1c("tpl");
}

#[test]
fn acid_tpl_imp() {
    common::imp("tpl");
}

#[test]
fn acid_tpl_otv() {
    common::otv("tpl");
}

#[test]
fn acid_tpl_fr() {
    common::fr("tpl");
}

#[test]
fn acid_tpl_lu() {
    common::lu("tpl");
}

#[test]
fn acid_tpl_g2item() {
    common::g2item("tpl");
}
