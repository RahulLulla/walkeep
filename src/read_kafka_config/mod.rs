use configparser::ini::Ini;

use crate::walkeep_errors::ReadConfigError;

pub struct KafkaConfig {
    pub broker: String,
    pub topic: String,
}

pub fn read_config() -> Result<KafkaConfig, Box<dyn std::error::Error>> {
    let mut config = Ini::new();
    config
        .load("config.ini")
        .or(Err(ReadConfigError::FileMissing))?;

    let load_config = |name| -> Result<String, ReadConfigError> {
        match config.get("KAFKA_CONFIG", name) {
            Some(data) => Ok(data),
            None => Err(ReadConfigError::MissingField(name)),
        }
    };

    Ok(KafkaConfig {
        broker: load_config("broker")?,
        topic: load_config("topic")?,
    })
}
