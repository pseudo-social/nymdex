/// Initialize our logger with default settings automatically
pub fn init(level: Option<String>) {
  env_logger::init_from_env(
    env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, level.unwrap_or("info".into())));
}

