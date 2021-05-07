pub mod common;

pub mod storage;

pub mod scheduler;

pub mod workloads;

pub mod gpc;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
