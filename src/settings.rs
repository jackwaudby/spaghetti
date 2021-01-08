use config::{Config, ConfigError, Environment, File};

pub struct General {
    pub workload: String,
}

pub struct Log {
    pub level: String,
}

pub struct TpcC {
    pub warehouses: u16,
    pub districts: u16,
}

const CONFIG_FILE_PATH: &str = "./config/Default.toml";

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();

        s.merge(File::with_name(CONFIG_FILE_PATH))?;

        s.try_into()
    }
}
