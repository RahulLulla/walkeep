//! Custom Error Types
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ReadConfigError {
    #[error("File config.ini not found")]
    FileMissing,
    #[error("There was an issue reading config.ini, Field {0} not found")]
    MissingField(&'static str),
    #[error("There was an issue reading config.ini, Port not parsable, check type")]
    InvalidPort,
}
