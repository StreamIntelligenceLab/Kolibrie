use std::sync::atomic::AtomicBool;

pub static GPU_MODE_ENABLED: AtomicBool = AtomicBool::new(false);
pub mod dictionary;
pub mod triple;