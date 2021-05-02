pub mod common;

pub mod datagen;

pub mod client;

pub mod server;

pub mod workloads;

pub mod gpc;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
