use crate::storage::datatype::Data;
use crate::workloads::acid::AcidDatabase;
use crate::Result;

use tracing::info;

pub fn populate_tables(population: usize, database: &mut AcidDatabase) -> Result<()> {
    // person
    let mut ws_flag = true;
    let person = database.get_mut_table(0);
    for pid in 0..population {
        person
            .get_tuple(0, pid)
            .get()
            .init_value(Data::Uint(pid as u64))?; // p_id
        person.get_tuple(1, pid).get().init_value(Data::Uint(1))?; // version
        person.get_tuple(2, pid).get().init_value(Data::Uint(0))?; // num_friends

        if ws_flag {
            person.get_tuple(3, pid).get().init_value(Data::Int(70))?; // balance
            ws_flag = false;
        } else {
            person.get_tuple(3, pid).get().init_value(Data::Int(80))?; // balance
            ws_flag = true;
        }
    }

    // knows
    let knows = database.get_mut_table(1);
    let mut p1id = 0;
    let mut p2id = 1;
    for i in 0..population / 2 {
        knows.get_tuple(0, i).get().init_value(Data::Uint(p1id))?; // p1_id
        knows.get_tuple(1, i).get().init_value(Data::Uint(p2id))?; // p1_id
        p1id += 2;
        p2id += 2;
    }

    info!("Loaded {} row(s) into knows", population / 2);

    Ok(())
}
