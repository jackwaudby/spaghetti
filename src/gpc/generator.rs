use crate::common::parameter_generation::ParameterGenerator;
//use crate::workloads::acid::paramgen::{
//     AcidGenerator, AcidTransactionProfile, G0Read, G2itemRead, LostUpdateRead,
// };
//use crate::workloads::acid::{AcidTransaction, ACID_SF_MAP};
use crate::workloads::acid::paramgen::AcidGenerator;
use crate::workloads::smallbank::paramgen::SmallBankGenerator;
use crate::workloads::tatp::paramgen::TatpGenerator;

use config::Config;

use std::sync::Arc;

/// Get transaction generator
pub fn get_transaction_generator(config: Arc<Config>) -> ParameterGenerator {
    let sf = config.get_int("scale_factor").unwrap() as u64;
    let set_seed = config.get_bool("set_seed").unwrap();
    let seed;
    if set_seed {
        let s = config.get_int("seed").unwrap() as u64;
        seed = Some(s);
    } else {
        seed = None;
    }
    match config.get_str("workload").unwrap().as_str() {
        "acid" => {
            let anomaly = config.get_str("anomaly").unwrap();
            let delay = config.get_int("delay").unwrap() as u64;
            let gen = AcidGenerator::new(sf, set_seed, seed, &anomaly, delay);
            ParameterGenerator::Acid(gen)
        }
        "tatp" => {
            let use_nurand = config.get_bool("nurand").unwrap();
            let gen = TatpGenerator::new(sf, set_seed, seed, use_nurand);
            ParameterGenerator::Tatp(gen)
        }
        "smallbank" => {
            let use_balance_mix = config.get_bool("use_balance_mix").unwrap();
            let gen = SmallBankGenerator::new(sf, set_seed, seed, use_balance_mix);
            ParameterGenerator::SmallBank(gen)
        }
        _ => unimplemented!(),
    }
}

// /// Run generator.
// pub fn run(&mut self) {
//     let max_transactions = config.get_int("transactions").unwrap() as u32;
//     let timeout = config.get_int("timeout").unwrap() as u64;
//     let sf = config.get_int("scale_factor").unwrap() as u64;
//     let set_seed = config.get_bool("set_seed").unwrap();
//     let seed;
//     if set_seed {
//         let s = config.get_int("seed").unwrap() as u64;
//         seed = Some(s);
//     } else {
//         seed = None;
//     }
//     let mut generator = match config.get_str("workload").unwrap().as_str() {
//         "acid" => {
//             let anomaly = config.get_str("anomaly").unwrap();
//             let delay = config.get_int("delay").unwrap() as u64;
//             let gen = AcidGenerator::new(sf, set_seed, seed, &anomaly, delay);
//             ParameterGenerator::Acid(gen)
//         }
//         "tatp" => {
//             let use_nurand = config.get_bool("nurand").unwrap();
//             let gen = TatpGenerator::new(sf, set_seed, seed, use_nurand);
//             ParameterGenerator::Tatp(gen)
//         }
//         "smallbank" => {
//             let use_balance_mix = config.get_bool("use_balance_mix").unwrap();
//             let gen = SmallBankGenerator::new(sf, set_seed, seed, use_balance_mix);
//             ParameterGenerator::SmallBank(gen)
//         }
//         _ => unimplemented!(),
//     };

//     let mut sent = 0;

//     // Set timeout
//     let st = Instant::now();
//     let runtime = Duration::new(timeout * 60, 0);
//     let et = st + runtime;

//     while let Ok(()) = self.next_rx.recv() {
//         if sent == max_transactions {
//             info!("All transactions sent: {} = {}", sent, max_transactions);

//             // ACID TEST ONLY -- post-execution recon queries
//             let workload = config.get_str("workload").unwrap();
//             if workload.as_str() == "acid" {
//                 let anomaly = config.get_str("anomaly").unwrap(); // get anomaly
//                 std::thread::sleep(std::time::Duration::from_millis(5000)); // artifical delay
//                 let sf = config.get_int("scale_factor").unwrap() as u64; // get sf
//                 let persons = *ACID_SF_MAP.get(&sf).unwrap(); // get persons

//                 match anomaly.as_str() {
//                     "g0" => {
//                         info!("Waiting to send G0Read");
//                         for p1_id in 0..persons {
//                             let p2_id = p1_id + 1;
//                             let payload = G0Read { p1_id, p2_id };

//                             let message = Message::Request {
//                                 request_no: generator.get_generated() + 1,
//                                 transaction: Transaction::Acid(AcidTransaction::G0Read),
//                                 parameters: Parameters::Acid(AcidTransactionProfile::G0Read(
//                                     payload,
//                                 )),
//                             };

//                             let request = match message {
//                                 Message::Request {
//                                     request_no,
//                                     transaction,
//                                     parameters,
//                                 } => InternalRequest {
//                                     request_no,
//                                     transaction,
//                                     parameters,
//                                     response_sender: self.logger_tx.clone(),
//                                 },
//                                 _ => unimplemented!(),
//                             };
//                             self.req_tx.send(request).unwrap();
//                         }
//                     }
//                     "lu" => {
//                         info!("Waiting to send LostUpdateRead");
//                         for p_id in 0..persons {
//                             let payload = LostUpdateRead { p_id };

//                             let message = Message::Request {
//                                 request_no: generator.get_generated() + 1,
//                                 transaction: Transaction::Acid(AcidTransaction::LostUpdateRead),
//                                 parameters: Parameters::Acid(
//                                     AcidTransactionProfile::LostUpdateRead(payload),
//                                 ),
//                             };

//                             let request = match message {
//                                 Message::Request {
//                                     request_no,
//                                     transaction,
//                                     parameters,
//                                 } => InternalRequest {
//                                     request_no,
//                                     transaction,
//                                     parameters,
//                                     response_sender: self.logger_tx.clone(),
//                                 },
//                                 _ => unimplemented!(),
//                             };
//                             self.req_tx.send(request).unwrap();
//                         }
//                     }

//                     "g2item" => {
//                         info!("Waiting to send G2itemRead");
//                         let p = persons * 4;
//                         for p_id in (0..p).step_by(2) {
//                             let payload = G2itemRead {
//                                 p1_id: p_id,
//                                 p2_id: p_id + 1,
//                             };

//                             let message = Message::Request {
//                                 request_no: generator.get_generated() + 1,
//                                 transaction: Transaction::Acid(AcidTransaction::G2itemRead),
//                                 parameters: Parameters::Acid(AcidTransactionProfile::G2itemRead(
//                                     payload,
//                                 )),
//                             };

//                             let request = match message {
//                                 Message::Request {
//                                     request_no,
//                                     transaction,
//                                     parameters,
//                                 } => InternalRequest {
//                                     request_no,
//                                     transaction,
//                                     parameters,
//                                     response_sender: self.logger_tx.clone(),
//                                 },
//                                 _ => unimplemented!(),
//                             };
//                             self.req_tx.send(request).unwrap();
//                         }
//                     }
//                     _ => info!("No post-execution recon queries"),
//                 }
//             }
//             break;
//         } else if Instant::now() > et {
//             info!("Timeout reached: {} minute(s)", timeout);
//             break;
//         } else {
//             let message = generator.get_next();
//             let request = match message {
//                 Message::Request {
//                     request_no,
//                     transaction,
//                     parameters,
//                 } => InternalRequest {
//                     request_no,
//                     transaction,
//                     parameters,
//                     response_sender: self.logger_tx.clone(),
//                 },
//                 _ => unimplemented!(),
//             };

//             self.req_tx.send(request).unwrap();
//             sent += 1;
//         }
//     }
// }
