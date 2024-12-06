use crate::utils::current_timestamp;

#[derive(PartialEq, Debug, Clone)]
pub struct SlidingWindow {
    pub width: u64,
    pub slide: u64,
    pub last_evaluated: u64,
}

impl SlidingWindow {
    pub fn new(width: u64, slide: u64) -> Self {
        Self {
            width,
            slide,
            last_evaluated: current_timestamp(),
        }
    }
}
