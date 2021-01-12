use crate::storage::row::Row;

use crate::workloads::Internal;

use rand::rngs::ThreadRng;
use rand::Rng;

use std::sync::Arc;
use tracing::info;

/// Populate the `Subscriber` table.
///
/// Schema:
/// Primary key: s_id
pub fn populate_subscriber_table(data: &Internal, rng: &mut ThreadRng) {
    info!("Loading subscriber table");
    let s_name = String::from("subscriber");
    let t = data.tables.get(&s_name).unwrap();
    let i_name = t.get_primary_index().unwrap();
    let i = data.indexes.get(&i_name).unwrap();

    let subs = data.config.get_int("subscribers").unwrap() as u64;
    for s_id in 0..subs {
        let mut row = Row::new(Arc::clone(&t));
        row.set_primary_key(s_id);
        row.set_value("s_id", s_id.to_string());
        row.set_value("sub_nbr", to_sub_nbr(s_id));
        row.set_value("bit_1", rng.gen_range(0, 1 + 1).to_string());
        row.set_value("bit_2", rng.gen_range(0, 1 + 1).to_string());
        row.set_value("bit_3", rng.gen_range(0, 1 + 1).to_string());
        row.set_value("bit_4", rng.gen_range(0, 1 + 1).to_string());
        row.set_value("bit_5", rng.gen_range(0, 1 + 1).to_string());
        row.set_value("bit_6", rng.gen_range(0, 1 + 1).to_string());
        row.set_value("bit_7", rng.gen_range(0, 1 + 1).to_string());
        row.set_value("bit_8", rng.gen_range(0, 1 + 1).to_string());
        row.set_value("bit_9", rng.gen_range(0, 1 + 1).to_string());
        row.set_value("bit_10", rng.gen_range(0, 1 + 1).to_string());
        row.set_value("hex_1", rng.gen_range(0, 15 + 1).to_string());
        row.set_value("hex_2", rng.gen_range(0, 15 + 1).to_string());
        row.set_value("hex_3", rng.gen_range(0, 15 + 1).to_string());
        row.set_value("hex_4", rng.gen_range(0, 15 + 1).to_string());
        row.set_value("hex_5", rng.gen_range(0, 15 + 1).to_string());
        row.set_value("hex_6", rng.gen_range(0, 15 + 1).to_string());
        row.set_value("hex_7", rng.gen_range(0, 15 + 1).to_string());
        row.set_value("hex_8", rng.gen_range(0, 15 + 1).to_string());
        row.set_value("hex_9", rng.gen_range(0, 15 + 1).to_string());
        row.set_value("hex_10", rng.gen_range(0, 15 + 1).to_string());
        row.set_value("byte_2_1", rng.gen_range(0, 255 + 1).to_string());
        row.set_value("byte_2_2", rng.gen_range(0, 255 + 1).to_string());
        row.set_value("byte_2_3", rng.gen_range(0, 255 + 1).to_string());
        row.set_value("byte_2_4", rng.gen_range(0, 255 + 1).to_string());
        row.set_value("byte_2_5", rng.gen_range(0, 255 + 1).to_string());
        row.set_value("byte_2_6", rng.gen_range(0, 255 + 1).to_string());
        row.set_value("byte_2_7", rng.gen_range(0, 255 + 1).to_string());
        row.set_value("byte_2_8", rng.gen_range(0, 255 + 1).to_string());
        row.set_value("byte_2_9", rng.gen_range(0, 255 + 1).to_string());
        row.set_value("byte_2_10", rng.gen_range(0, 255 + 1).to_string());
        row.set_value("msc_location", rng.gen_range(1, 2 ^ 32 - 1).to_string());
        row.set_value("vlr_location", rng.gen_range(1, 2 ^ 32 - 1).to_string());
        i.index_insert(s_id, row);
    }
}

// TODO: move to helper
pub fn to_sub_nbr(s_id: u64) -> String {
    let mut num = s_id.to_string();
    for _i in 0..15 {
        if num.len() == 15 {
            break;
        }
        num = format!("0{}", num);
    }
    num
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn to_sub_nbr_test() {
        let s_id = 40958;
        let sub_nbr = to_sub_nbr(s_id);
        info!("{:?}", sub_nbr.to_string().len());
        assert_eq!(sub_nbr.len(), 15);
    }
}
