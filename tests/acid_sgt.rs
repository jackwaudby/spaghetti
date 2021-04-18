use spaghetti::server::storage::datatype::SuccessMessage;

use std::fs::File;
use std::io::{prelude::*, BufReader};
use test_env_log::test;

mod common;

#[test]
fn acid_sgt_g1a() {
    let config = common::setup_config("sgt");
    common::run(config);

    let f = format!("./log/acid/sgt/g1a.json");
    let file = File::open(f).unwrap();
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let resp: SuccessMessage = serde_json::from_str(&line.unwrap()).unwrap();
        let version = resp
            .get_values()
            .get("version")
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert_eq!(version, 1, "expected: {}, actual: {}", version, 1);
    }
}
