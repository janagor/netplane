use std::time::Duration;

pub const RESPONSE_LIMIT: usize = 1024 * 1024;
pub const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(10);

pub fn origin(variable: &str, default: &str) -> Result<String, std::env::VarError> {
    match std::env::var(variable) {
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(error) => Err(error),
    }
}
