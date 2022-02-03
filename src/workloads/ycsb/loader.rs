use crate::storage::datatype::Data;
use crate::workloads::ycsb::helper;
use crate::workloads::ycsb::YcsbDatabase;
use crate::Result;

use rand::rngs::StdRng;

use tracing::info;

pub fn populate_tables(
    population: usize,
    database: &mut YcsbDatabase,
    rng: &mut StdRng,
) -> Result<()> {
    for offset in 0..population {
        database.insert_value(0, 0, offset, Data::Uint(offset as u64)); // key

        let s1 = helper::generate_random_string(rng);
        let s2 = helper::generate_random_string(rng);
        let s3 = helper::generate_random_string(rng);
        let s4 = helper::generate_random_string(rng);
        let s5 = helper::generate_random_string(rng);
        let s6 = helper::generate_random_string(rng);
        let s7 = helper::generate_random_string(rng);
        let s8 = helper::generate_random_string(rng);
        let s9 = helper::generate_random_string(rng);
        let s10 = helper::generate_random_string(rng);

        database.insert_value(0, 1, offset, Data::VarChar(s1));
        database.insert_value(0, 2, offset, Data::VarChar(s2));
        database.insert_value(0, 3, offset, Data::VarChar(s3));
        database.insert_value(0, 4, offset, Data::VarChar(s4));
        database.insert_value(0, 5, offset, Data::VarChar(s5));
        database.insert_value(0, 6, offset, Data::VarChar(s6));
        database.insert_value(0, 7, offset, Data::VarChar(s7));
        database.insert_value(0, 8, offset, Data::VarChar(s8));
        database.insert_value(0, 9, offset, Data::VarChar(s9));
        database.insert_value(0, 10, offset, Data::VarChar(s10));
    }
    info!("Loaded {} rows into table 1", population);

    Ok(())
}
