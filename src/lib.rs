pub mod parameter_generation;

pub mod scheduler;

pub mod storage;

pub mod client;

pub mod server;

pub mod shutdown;

pub mod frame;

pub mod connection;

pub mod transaction;

pub mod workloads;

/// Error handling approach: boxing errors.
/// Pros: simple and allows original errors to be preserved.
/// Cons: the underlying error type is only know at runtime and is not statically determined.
///
/// Box converts any type that implements the Error trait into the trait object Box<Error> using From.
///
/// For code on the normal operation path boxing is avoided, e.g., in frame.rs a custom parse error is defined so error type can be known at runtime.
/// std::error::Error is implemented for parse::Error so it can be converted to a boxed error.
///
/// TODO: explore using a specific error type for spaghetti, wrapping all errors.
pub type SpagError = Box<dyn std::error::Error + Send + Sync>;

/// Result type alias.
pub type Result<T> = std::result::Result<T, SpagError>;
