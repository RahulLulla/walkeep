use configparser::ini::Ini;
use pgwire_replication::ReplicationConfig;

use crate::walkeep_errors::ReadConfigError;

/// Function is for purpose of reading configuration from config.ini file
/// and creating a ReplicationConfig struct for establishing connection
pub fn read_config() -> Result<ReplicationConfig, Box<dyn std::error::Error>> {
    let mut config = Ini::new();
    config
        .load("config.ini")
        .or(Err(ReadConfigError::FileMissing))?;

    let load_config = |name| -> Result<String, ReadConfigError> {
        match config.get("DB_CONFIG", name) {
            Some(data) => Ok(data),
            None => Err(ReadConfigError::MissingField(name)),
        }
    };

    Ok(ReplicationConfig {
        host: load_config("db_host")?,
        port: load_config("db_port")?
            .parse()
            .or(Err(ReadConfigError::InvalidPort))?,
        user: load_config("walkeep_user")?,
        password: load_config("secure_pass")?,
        database: load_config("db_name")?,
        slot: load_config("walkeep_slot")?,
        publication: load_config("walkeep_pub")?,
        ..Default::default()
    })
}
