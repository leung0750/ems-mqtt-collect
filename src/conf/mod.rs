use config::{Config, ConfigError, FileFormat};
use once_cell::sync::OnceCell;

// 全局配置实例，只加载一次
static GLOBAL_CONFIG: OnceCell<Config> = OnceCell::new();

pub fn load_config<T: serde::de::DeserializeOwned>(section: &str) -> Result<T, ConfigError> {
    // 尝试获取全局配置，如果不存在则加载
    let settings = GLOBAL_CONFIG.get_or_try_init(|| {
        Config::builder()
            .add_source(config::File::with_name("conf.ini").format(FileFormat::Ini))
            .build()
    })?;
    settings.get(section).map_err(|e| e.into())
}
