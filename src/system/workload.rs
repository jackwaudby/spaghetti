use rand::rngs::ThreadRng;
use std::error::Error;

pub trait Workload {
    fn init(filename: &str) -> Result<Self, Box<dyn Error>>
    where
        Self: std::marker::Sized;

    fn populate_tables(&self, rng: &mut ThreadRng);
}
