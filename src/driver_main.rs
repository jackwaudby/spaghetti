use benchmarks::tpcc::helper;
use benchmarks::tpcc::TpcC;
use configuration::SETTINGS;
use server::system::workload::Workload;
use std::sync::Arc;

fn main() {
    let workload: String = SETTINGS.get_workload();

    let w = match workload.as_str() {
        "tpcc" => {
            log::info!("initialise tpc-c workload");
            match TpcC::init("./benchmarks/tpcc_short_schema.txt") {
                Ok(w) => w,
                Err(e) => panic!("{}", e),
            }
        }
        _ => panic!("something else!"),
    };

    log::info!("populate tables");
    let mut rng = rand::thread_rng();
    w.populate_tables(&mut rng);
    log::info!("tables loaded!");

    let params = benchmarks::tpcc::new_order::NewOrder::new(
        1,
        SETTINGS.get_warehouses(),
        SETTINGS.get_districts(),
        &mut rng,
    );
    log::info!("generate parameters: {}", params);
    let w_tax = new_order_part_1(params.get_w_id(), &w);
    let (d_tax, d_next_o_id) = new_order_part_2(params.get_w_id(), params.get_d_id(), &w);
    let (c_discount, c_last) =
        new_order_part_3(params.get_w_id(), params.get_d_id(), params.get_c_id(), &w);
    log::info!("send to worker queue");
}

fn new_order_part_1(w_id: u64, w: &TpcC) -> String {
    log::info!("SELECT w_tax FROM warehouse WHERE w_id={};", w_id);
    let key = w_id;
    let index = Arc::clone(&w.indexes.get("warehouse_idx").unwrap());
    let row = index.index_read(key).unwrap();
    row.get_value("w_tax".to_string()).unwrap()
}

fn new_order_part_2(w_id: u64, d_id: u64, w: &TpcC) -> (String, String) {
    let q1 = format!(
        "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id={} AND d_id={};",
        w_id, d_id
    );
    let q2 = format!(
        "UPDATE district SET d_next_o_id = d_next_o_id +1 WHERE d_w_id={} AND d_id={};",
        w_id, d_id
    );

    log::info!("{}", q1);
    log::info!("{}", q2);
    let key = helper::district_key(w_id, d_id);
    let index = Arc::clone(&w.indexes.get("district_idx").unwrap());
    let mut row = index.index_read_mut(key).unwrap();
    let d_tax = row.get_value("d_tax".to_string()).unwrap();
    let d_next_o_id = row.get_value("d_next_o_id".to_string()).unwrap();
    let new_d_next_o_id = d_next_o_id.parse::<u64>().unwrap() + 1;
    row.set_value("d_next_o_id", new_d_next_o_id.to_string());
    (d_tax, d_next_o_id)
}

fn new_order_part_3(w_id: u64, d_id: u64, c_id: u64, w: &TpcC) -> (String, String) {
    let q1 = format!(
        "SELECT c_discount, c_last, c_credit FROM customer WHERE c_id={} AND c_d_id={} AND c_w_id={};",
      d_id, c_id  ,w_id
    );

    let key = helper::customer_key(w_id, d_id, c_id);
    let index = Arc::clone(&w.indexes.get("customer_id_idx").unwrap());
    let mut row = index.index_read(key).unwrap();
    let c_discount = row.get_value("c_discount".to_string()).unwrap();
    let c_last = row.get_value("c_last".to_string()).unwrap();
    // let c_credit = row.get_value("c_credit".to_string()).unwrap();
    // (c_discount, c_last, c_credit)
    (c_discount, c_last)
}
