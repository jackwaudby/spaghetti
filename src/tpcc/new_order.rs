use crate::tpcc::helper;
use std::fmt;

use log::*;
use rand::Rng;

const UNUSED: u64 = u64::MAX;

#[derive(Debug)]
pub struct NewOrder {
    w_id: u64, // home warehouse id
    d_id: u64,
    c_id: u64,
    ol_cnt: u64, // number of lines in order
    rbk: u64,    // indicates a rollback when = 1
    orderlines: Vec<OrderLine>,
}

impl NewOrder {
    /// Generate parameters for `NewOrder` transaction.
    pub fn new<T: Rng>(w_id: u64, warehouses: u64, districts: u64, rng: &mut T) -> Self {
        let d_id = helper::rand(1, districts, rng);
        let c_id = helper::nu_rand(1, 3000, rng);
        let ol_cnt = helper::rand(5, 15, rng);
        let rbk = helper::rand(1, 100, rng);
        let mut orderlines = vec![];

        for orderline in 0..ol_cnt {
            if rbk == 1 && orderline == ol_cnt - 1 {
                orderlines.push(OrderLine::new(warehouses, w_id, true, rng));
            } else {
                orderlines.push(OrderLine::new(warehouses, w_id, false, rng));
            }
        }

        NewOrder {
            w_id,
            d_id,
            c_id,
            ol_cnt,
            rbk,
            orderlines,
        }
    }

    /// Get warehouse id of new order.
    pub fn get_w_id(&self) -> u64 {
        self.w_id
    }

    /// Get district id of new order.
    pub fn get_d_id(&self) -> u64 {
        self.d_id
    }

    /// Get customer id of new order.
    pub fn get_c_id(&self) -> u64 {
        self.c_id
    }
}

// [warehouse id, district id, cust id, orderline, rbk]
impl fmt::Display for NewOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "[{},{},{},{},{}]",
            self.w_id, self.d_id, self.c_id, self.ol_cnt, self.rbk
        )
    }
}

#[derive(Debug)]
struct OrderLine {
    ol_i_id: u64,
    ol_supply_w_id: u64,
    ol_quantity: u64,
}

impl OrderLine {
    /// Generate a new `OrderLine` for an `Order`.
    fn new<T: Rng>(warehouses: u64, w_id: u64, rbk: bool, rng: &mut T) -> Self {
        log::debug!("warehouses: {}, w_id: {}, rbk: {}", warehouses, w_id, rbk);

        let ol_i_id;
        if rbk {
            ol_i_id = UNUSED;
        } else {
            ol_i_id = helper::nu_rand(1, 100000, rng);
        }

        let supply_warehouse = helper::rand(1, 100, rng);
        log::debug!("supplying warehouse rand: {}", supply_warehouse);

        let ol_supply_w_id;
        if supply_warehouse == 1 {
            let warehouse_ids: Vec<u64> = (0..warehouses).filter(|&x| x != w_id).collect();
            let ind = helper::rand(0, warehouse_ids.len() as u64, rng);
            ol_supply_w_id = warehouse_ids[ind as usize];
        } else {
            ol_supply_w_id = w_id;
        }

        let ol_quantity = helper::rand(1, 10, rng);

        OrderLine {
            ol_i_id,
            ol_supply_w_id,
            ol_quantity,
        }
    }
}

// [item id, supplying warehouse id, quantity]
impl fmt::Display for OrderLine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "[{},{},{}]",
            self.ol_i_id, self.ol_supply_w_id, self.ol_quantity
        )
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn gen_params() {
        env_logger::init();

        let mut rng = StdRng::seed_from_u64(42);

        let ol_rbk = OrderLine::new(10, 1, true, &mut rng);
        assert_eq!(
            format!("{}", ol_rbk),
            "[18446744073709551615,1,5]".to_string()
        );
        let ol_home = OrderLine::new(10, 2, false, &mut rng);
        assert_eq!(format!("{}", ol_home), "[17185,2,9]".to_string());

        let no = NewOrder::new(1, 10, 10, &mut rng);
        assert_eq!(format!("{}", no), "[1,5,2238,15,63]".to_string());
    }
}
