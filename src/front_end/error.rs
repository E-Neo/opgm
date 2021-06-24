use super::GispRule;

pub type Result<T> = std::result::Result<T, pest::error::Error<GispRule>>;
