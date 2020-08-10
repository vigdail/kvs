mod command;
mod engines;
mod error;

pub use crate::engines::{KvStore, KvsEngine};
pub use crate::error::Error;

pub type Result<T> = std::result::Result<T, Error>;
