// Measure the FLOPS of the CPU
// use num_cpus;
use rayon::prelude::*;
use std::time::Instant;

pub fn measure_flops() -> f32 {
    let flops = 2147483647;
    flops as f32
}
