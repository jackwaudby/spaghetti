use crate::server::storage::row::Row;
use crate::workloads::tpcc::helper;
use crate::workloads::tpcc::keys::TpccPrimaryKey;
use crate::workloads::Internal;
use crate::workloads::PrimaryKey;
use crate::Result;
use rand::rngs::StdRng;
use rand::Rng;
use std::sync::Arc;
use tracing::info;
// const UNUSED: u64 = u64::MAX;

//////////////////////////////
/// Table Loaders. ///
//////////////////////////////

/// Populate tables.
pub fn populate_tables(data: &Internal, rng: &mut StdRng) -> Result<()> {
    populate_warehouse_table(data, rng)?;
    populate_item_table(data, rng)?;
    populate_stock_table(data, rng)?;
    populate_district_table(data, rng)?;
    populate_customer_table(data, rng)?;
    Ok(())
}

/// Populate the `Item` table.
///
/// Schema: (int,i_id) (int,i_im_id) (string,i_name) (double,i_price) (string,i_data)
/// Primary key: i_id
fn populate_item_table(data: &Internal, rng: &mut StdRng) -> Result<()> {
    info!("Loading item table");
    let t_name = "item";
    let t = data.get_table(t_name)?;
    let i_name = t.get_primary_index()?;
    let i = data.get_index(&i_name)?;

    let max_items = data.config.get_int("max_items")? as u64;

    for i_id in 0..max_items + 1 {
        let mut row = Row::new(Arc::clone(&t));
        let pk = PrimaryKey::Tpcc(TpccPrimaryKey::Item(i_id));
        row.set_primary_key(pk);
        row.set_value("i_id", &i_id.to_string())?;
        row.set_value("i_im_id", &rng.gen_range(1..=10000).to_string())?;
        row.set_value("i_name", &helper::random_string(14, 24, rng))?;
        row.set_value("i_price", &helper::random_float(1.0, 100.0, 2, rng))?;
        row.set_value("i_data", &helper::item_data(rng))?;
        i.index_insert(pk, row)?;
    }
    Ok(())
}

/// Populate the `Warehouse` table.
///
/// Schema: (int,w_id) (string,w_name) (string,w_street_1) (string,w_street_2) (string,w_city) (string,w_state) (string,w_zip)
/// (double,w_tax) (double,w_ytd)
/// Primary key: w_id
fn populate_warehouse_table(data: &Internal, rng: &mut StdRng) -> Result<()> {
    info!("Loading warehouse table");
    let n = data.config.get_int("warehouses")? as u64;
    let t_name = "warehouse";
    let t = data.get_table(t_name)?;
    let i_name = t.get_primary_index()?;
    let i = data.get_index(&i_name)?;

    for w_id in 0..n {
        let mut row = Row::new(Arc::clone(&t));
        let pk = PrimaryKey::Tpcc(TpccPrimaryKey::Warehouse(w_id));
        row.set_primary_key(pk);
        row.set_value("w_id", &w_id.to_string())?;
        row.set_value("w_name", &helper::random_string(6, 10, rng))?;
        row.set_value("w_street_1", &helper::random_string(10, 20, rng))?;
        row.set_value("w_street_2", &helper::random_string(10, 20, rng))?;
        row.set_value("w_city", &helper::random_string(10, 20, rng))?;
        row.set_value("w_state", &helper::random_string(2, 2, rng))?;
        row.set_value("w_zip", &helper::zip(rng))?;
        row.set_value("w_tax", &helper::random_float(0.0, 0.2, 4, rng))?;
        row.set_value("w_ytd", "300000.0")?;
        i.index_insert(pk, row)?;
    }
    Ok(())
}

/// Populate the `District` table.
///
/// Schema: (int,d_id) (int,d_w_id) (string,d_name) (string,d_street_1) (string,d_street_2) (string,d_city) (string,d_state)
/// (string,d_zip) (double,d_tax) (double,d_ytd) (int,d_next_o_id)
/// Primary key:
fn populate_district_table(data: &Internal, rng: &mut StdRng) -> Result<()> {
    info!("Loading district table");
    let t_name = "district";
    let t = data.get_table(t_name)?;
    let i_name = t.get_primary_index()?;
    let i = data.get_index(&i_name)?;
    let n = data.config.get_int("warehouses")? as u64;
    let d = data.config.get_int("districts")? as u64;

    for w_id in 0..n {
        for d_id in 0..d {
            let mut row = Row::new(Arc::clone(&t));
            let pk = PrimaryKey::Tpcc(TpccPrimaryKey::District(w_id, d_id));
            row.set_primary_key(pk);
            row.set_value("d_id", &d_id.to_string())?;
            row.set_value("d_w_id", &w_id.to_string())?;
            row.set_value("d_name", &helper::random_string(6, 10, rng))?;
            row.set_value("d_street_1", &helper::random_string(10, 20, rng))?;
            row.set_value("d_street_2", &helper::random_string(10, 20, rng))?;
            row.set_value("d_city", &helper::random_string(10, 20, rng))?;
            row.set_value("d_state", &helper::random_string(2, 2, rng))?;
            row.set_value("d_zip", &helper::zip(rng))?;
            row.set_value("d_tax", &helper::random_float(0.0, 0.2, 4, rng))?;
            row.set_value("d_ytd", "30000.0")?;
            row.set_value("d_next_o_id", "3001")?;
            i.index_insert(pk, row)?;
        }
    }
    Ok(())
}

/// Populate the `Stock` table.
///
/// Schema: (int,s_i_id) (int,s_w_id) (int,s_quantity) (int,s_remote_cnt)
/// Primary key: (s_i_id,s_w_id)
fn populate_stock_table(data: &Internal, rng: &mut StdRng) -> Result<()> {
    info!("loading stock table");
    let t_name = "stock";
    let t = data.get_table(t_name)?;
    let i_name = t.get_primary_index()?;
    let i = data.get_index(&i_name)?;

    let n = data.config.get_int("warehouses")? as u64;

    let mi = data.config.get_int("max_items")? as u64;

    for w_id in 0..n {
        for s_i_id in 0..mi {
            let mut row = Row::new(Arc::clone(&t));
            let pk = PrimaryKey::Tpcc(TpccPrimaryKey::Stock(w_id, s_i_id));
            row.set_primary_key(pk);
            row.set_value("s_i_id", &s_i_id.to_string())?;
            row.set_value("s_w_id", &w_id.to_string())?;
            row.set_value("s_quantity", &rng.gen_range(10..=100).to_string())?;
            row.set_value("s_remote_cnt", "0")?;
            i.index_insert(pk, row)?;
        }
    }
    Ok(())
}

/// Populate the `Customer` table.
///
/// Schema: (int,c_id) (int,c_d_id) (int,c_w_id) (string,c_middle) (string,c_last) (string,c_state) (string,c_credit) (int,c_discount)
/// (double,c_balance) (double,c_ytd_payment) (int,c_payment_cnt)
/// Primary key: c_id
fn populate_customer_table(data: &Internal, rng: &mut StdRng) -> Result<()> {
    info!("Loading customer table");
    let t_name = "customer";
    let t = data.get_table(t_name)?;
    let i_name = t.get_primary_index()?;
    let i = data.get_index(&i_name)?;

    let w = data.config.get_int("warehouses")? as u64;
    let d = data.config.get_int("districts")? as u64;
    let c = data.config.get_int("customers")? as u64;

    for w_id in 0..w {
        for d_id in 0..d {
            for c_id in 0..c {
                let mut row = Row::new(Arc::clone(&t));
                let pk = PrimaryKey::Tpcc(TpccPrimaryKey::Customer(w_id, d_id, c_id));
                row.set_primary_key(pk);
                row.set_value("c_id", &c_id.to_string())?;
                row.set_value("c_d_id", &d_id.to_string())?;
                row.set_value("c_w_id", &w_id.to_string())?;
                row.set_value("c_last", &helper::last_name(c_id, rng))?;
                row.set_value("c_discount", &helper::random_float(0.0, 0.5, 4, rng))?;
                row.set_value("c_balance", "-10.0")?;
                row.set_value("c_ytd_payment", "10.0")?;
                row.set_value("c_payment_cnt", "1")?;
                i.index_insert(pk, row)?;
            }
        }
    }
    Ok(())
}

//TODO: new order, order, history
