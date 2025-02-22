// Measure the FLOPS of the CPU
// use num_cpus;
use std::time::Instant;

pub fn measure_flops() -> f64 {
    let start = Instant::now();
    
    // Perform some floating-point operations
    let mut sum = 0.0;
    for i in 0..1_000_000 {
        sum += (i as f64).sqrt();
    }
    
    let duration = start.elapsed();
    
    // Calculate actual FLOPS based on operations performed
    let operations = 1_000_000.0; // One sqrt operation per iteration
    operations / duration.as_secs_f64()
}
