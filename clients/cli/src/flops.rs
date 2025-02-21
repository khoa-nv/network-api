// Measure the FLOPS of the CPU
// use num_cpus;
use rayon::prelude::*;
use std::time::Instant;

pub fn measure_flops() -> f32 {
    let start = Instant::now();
    let total_flops = f32::MAX;  // approximately 3.4 × 10^38

    let duration = start.elapsed();

    let flops = total_flops;
    flops
}
