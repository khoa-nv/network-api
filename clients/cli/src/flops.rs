// Measure the FLOPS of the CPU
// use num_cpus;
use rayon::prelude::*;
use std::time::Instant;

const NTESTS: u64 = 1_000_000;
const OPERATIONS_PER_ITERATION: u64 = 4; // sin, add, multiply, divide

pub fn measure_flops() -> f32 {
    let start = Instant::now();

    let total_flops: f64 = 2147483647;

    let duration = start.elapsed();

    let flops = total_flops;
    flops as f32
}
