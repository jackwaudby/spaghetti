use crate::common::message::Message;
use crate::common::parameter_generation::Generator;
use crate::workloads::tpcc::profiles::{NewOrder, TpccTransaction};

//////////////////////////////////////////
/// Parameter Generation. ///
//////////////////////////////////////////

/// Contains parameters needed for generation.
pub struct TpccGenerator {
    pub warehouses: u64,
    pub districts: u64,
}

impl TpccGenerator {
    // fn new(warehouses: u64, districts: u64) -> TpccGenerator {
    //     TpccGenerator {
    //         warehouses,
    //         districts,
    //     }
    // }
}

impl Generator for TpccGenerator {
    fn generate(&mut self) -> Message {
        Message::TpccTransaction(TpccTransaction::NewOrder(NewOrder { w_id: 1 }))
    }
    fn get_generated(&self) -> u32 {
        1
    }
}

// impl NewOrderParams {
//     /// Generate parameters for `NewOrder` transaction.
//     pub fn new<T: Rng>(w_id: u64, warehouses: u64, districts: u64, rng: &mut T) -> Self {
//         let d_id = helper::rand(1, districts, rng);
//         let c_id = helper::nu_rand(1, 3000, rng);
//         let ol_cnt = helper::rand(5, 15, rng);
//         let rbk = helper::rand(1, 100, rng);
//         let mut orderlines = vec![];

//         for orderline in 0..ol_cnt {
//             if rbk == 1 && orderline == ol_cnt - 1 {
//                 orderlines.push(OrderLine::new(warehouses, w_id, true, rng));
//             } else {
//                 orderlines.push(OrderLine::new(warehouses, w_id, false, rng));
//             }
//         }

//         NewOrderParams {
//             w_id,
//             d_id,
//             c_id,
//             ol_cnt,
//             rbk,
//             orderlines,
//         }
//     }

//     /// Get warehouse id of new order.
//     pub fn get_w_id(&self) -> u64 {
//         self.w_id
//     }

//     /// Get district id of new order.
//     pub fn get_d_id(&self) -> u64 {
//         self.d_id
//     }

//     /// Get customer id of new order.
//     pub fn get_c_id(&self) -> u64 {
//         self.c_id
//     }
// }

// // [warehouse id, district id, cust id, orderline, rbk]
// impl fmt::Display for NewOrderParams {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(
//             f,
//             "[{},{},{},{},{}]",
//             self.w_id, self.d_id, self.c_id, self.ol_cnt, self.rbk
//         )
//     }
// }

// #[derive(Debug, Deserialize, Serialize, PartialEq)]
// struct OrderLine {
//     ol_i_id: u64,
//     ol_supply_w_id: u64,
//     ol_quantity: u64,
// }

// impl OrderLine {
//     /// Generate a new `OrderLine` for an `Order`.
//     fn new<T: Rng>(warehouses: u64, w_id: u64, rbk: bool, rng: &mut T) -> Self {
//         debug!("warehouses: {}, w_id: {}, rbk: {}", warehouses, w_id, rbk);

//         let ol_i_id;
//         if rbk {
//             ol_i_id = UNUSED;
//         } else {
//             ol_i_id = helper::nu_rand(1, 100000, rng);
//         }

//         let supply_warehouse = helper::rand(1, 100, rng);
//         debug!("supplying warehouse rand: {}", supply_warehouse);

//         let ol_supply_w_id;
//         if supply_warehouse == 1 {
//             let warehouse_ids: Vec<u64> = (0..warehouses).filter(|&x| x != w_id).collect();
//             let ind = helper::rand(0, warehouse_ids.len() as u64, rng);
//             ol_supply_w_id = warehouse_ids[ind as usize];
//         } else {
//             ol_supply_w_id = w_id;
//         }

//         let ol_quantity = helper::rand(1, 10, rng);

//         OrderLine {
//             ol_i_id,
//             ol_supply_w_id,
//             ol_quantity,
//         }
//     }
// }

// // [item id, supplying warehouse id, quantity]
// impl fmt::Display for OrderLine {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(
//             f,
//             "[{},{},{}]",
//             self.ol_i_id, self.ol_supply_w_id, self.ol_quantity
//         )
//     }
// }

// #[cfg(test)]
// mod tests {

//     use super::*;

//     use rand::rngs::StdRng;
//     use rand::SeedableRng;
//     use std::sync::Arc;

//     #[test]
//     fn gen_params() {
//         let mut rng = StdRng::seed_from_u64(42);

//         let ol_rbk = OrderLine::new(10, 1, true, &mut rng);
//         assert_eq!(
//             format!("{}", ol_rbk),
//             "[18446744073709551615,1,5]".to_string()
//         );
//         let ol_home = OrderLine::new(10, 2, false, &mut rng);
//         assert_eq!(format!("{}", ol_home), "[17185,2,9]".to_string());

//         let no = NewOrderParams::new(1, 10, 10, &mut rng);
//         assert_eq!(format!("{}", no), "[1,5,2238,15,63]".to_string());
//     }

//     #[test]
//     fn tpcc() {
//         // TABLE=WAREHOUSE
//         //     int64_t,W_ID
//         //     string,W_NAME
//         //     string,W_STREET_1
//         //     string,W_STREET_2
//         //     string,W_CITY
//         //     string,W_STATE
//         //     string,W_ZIP
//         //     double,W_TAX
//         //     double,W_YTD

//         // 0. Create atomic table map
//         let mut tables = HashMap::new(); // String: table_name, table: Table

//         // 1. Create schema and table
//         let table_name = String::from("warehouse");
//         let mut catalog = Catalog::init(&table_name, 1);
//         catalog.add_column(("w_id", "int"));
//         catalog.add_column(("w_name", "string"));
//         catalog.add_column(("w_street_1", "string"));
//         catalog.add_column(("w_street_2", "string"));
//         catalog.add_column(("w_city", "string"));
//         catalog.add_column(("w_state", "string"));
//         catalog.add_column(("w_zip", "string"));
//         catalog.add_column(("w_tax", "double"));
//         catalog.add_column(("w_ytd", "double"));
//         let w_table = Table::init(catalog);

//         // 2. Create index for table
//         let w_index = Index::init("warehouse_idx");
//         w_table.set_primary_index("warehouse_idx");

//         // 3. Allow table multiple owners
//         let w_table = Arc::new(w_table);

//         // 4. Put in table map
//         tables.insert(table_name, Arc::clone(&w_table));

//         // 5. Generate row and set fields
//         let mut row = Row::new(Arc::clone(&w_table));
//         row.set_value("w_id", "1".to_string());
//         row.set_value("w_name", "main".to_string());
//         row.set_value("w_street_1", "church lane".to_string());
//         row.set_value("w_street_2", "hedon".to_string());
//         row.set_value("w_city", "hull".to_string());
//         row.set_value("w_state", "east yorkshire".to_string());
//         row.set_value("w_zip", "hu11 8uz".to_string());
//         row.set_value("w_tax", "0.2".to_string());
//         row.set_value("w_ytd", "1000.0".to_string());

//         let mut row2 = Row::new(Arc::clone(&w_table));
//         row2.set_value("w_id", "2".to_string());
//         row2.set_value("w_name", "backup".to_string());
//         row2.set_value("w_street_1", "ganstead lane".to_string());
//         row2.set_value("w_street_2", "ganstead".to_string());
//         row2.set_value("w_city", "hull".to_string());
//         row2.set_value("w_state", "east yorkshire".to_string());
//         row2.set_value("w_zip", "hu11 4bg".to_string());
//         row2.set_value("w_tax", "0.2".to_string());
//         row2.set_value("w_ytd", "2000.0".to_string());

//         // 6. Assign row to index
//         w_index.index_insert(1, row);
//         w_index.index_insert(2, row2);

//         // 7. Change row field
//         w_index
//             .index_read_mut(1)
//             .unwrap()
//             .set_value("w_ytd", "3000.0".to_string());

//         assert_eq!(
//             format!("{}", *w_index.index_read(1).unwrap()),
//             String::from(
//                 "[0, 0, warehouse, 1, main, church lane, hedon, hull, east yorkshire, hu11 8uz, 0.2, 3000]"
//             )
//         );

//         assert_eq!(
//             format!("{}", *w_index.index_read(2).unwrap()),
//             String::from(
//                 "[1, 0, warehouse, 2, backup, ganstead lane, ganstead, hull, east yorkshire, hu11 4bg, 0.2, 2000]"
//             )
//         );
//     }
// }
