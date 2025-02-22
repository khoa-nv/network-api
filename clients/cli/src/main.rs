// Copyright (c) 2024 Nexus. All rights reserved.

mod analytics;
mod config;
// mod prover;
mod flops;
mod memory_stats;
#[path = "proto/nexus.orchestrator.rs"]
mod nexus_orchestrator;
mod node_id_manager;
mod orchestrator_client;
mod prover;
mod setup;
mod utils;

use clap::{Parser, Subcommand};
use tokio::signal::ctrl_c;
use std::sync::atomic::{AtomicBool, Ordering};

// Global flag for graceful shutdown
static SHUTDOWN_FLAG: AtomicBool = AtomicBool::new(false);

// Signal handler function
async fn handle_shutdown_signal() {
    ctrl_c().await.expect("Failed to listen for ctrl-c signal");
    println!("\nReceived shutdown signal. Initiating graceful shutdown...");
    SHUTDOWN_FLAG.store(true, Ordering::SeqCst);
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Environment {
    Local,
    Dev,
    Staging,
    Beta,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Command to execute
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the prover
    Start {
        /// Environment to run in
        #[arg(long, value_enum)]
        env: Option<Environment>,
    },
    /// Logout from the current session
    Logout,
}

#[derive(Parser, Debug)]
struct Args {
    /// Hostname at which Orchestrator can be reached
    hostname: String,

    /// Port over which to communicate with Orchestrator
    #[arg(short, long, default_value_t = 443u16)]
    port: u16,

    /// Whether to hang up after the first proof
    #[arg(short, long, default_value_t = false)]
    just_once: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Maximum number of restart attempts
    const MAX_RESTARTS: u32 = 100;
    // Initial delay between restarts in seconds
    const INITIAL_RETRY_DELAY: u64 = 5;
    // Maximum delay between restarts in seconds
    const MAX_RETRY_DELAY: u64 = 300; // 5 minutes

    let mut restart_count = 0;
    let mut retry_delay = INITIAL_RETRY_DELAY;

    // Spawn signal handler task
    tokio::spawn(handle_shutdown_signal());

    loop {
        if SHUTDOWN_FLAG.load(Ordering::SeqCst) {
            println!("Graceful shutdown initiated. Cleaning up...");
            break;
        }

        println!("Starting prover (attempt {})", restart_count + 1);

        let result = match &cli.command {
            Command::Start { env } => {
                prover::start_prover(&config::Environment::from_args(env.as_ref())).await
            }
            Command::Logout => {
                setup::clear_node_id().map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
            }
        };

        match result {
            Ok(_) => {
                if SHUTDOWN_FLAG.load(Ordering::SeqCst) {
                    println!("Process completed successfully, shutting down...");
                    break;
                }
                println!("Process completed, restarting...");
                restart_count = 0; // Reset counter on successful completion
                retry_delay = INITIAL_RETRY_DELAY; // Reset delay
                continue;
            }
            Err(e) => {
                eprintln!("Error occurred: {}", e);
                
                if SHUTDOWN_FLAG.load(Ordering::SeqCst) {
                    println!("Error occurred during shutdown, exiting...");
                    break;
                }

                if restart_count >= MAX_RESTARTS {
                    eprintln!("Maximum restart attempts ({}) reached. Exiting.", MAX_RESTARTS);
                    break;
                }

                restart_count += 1;
                println!("Restarting in {} seconds...", retry_delay);
                
                // Use timeout that can be interrupted by shutdown signal
                let sleep_future = tokio::time::sleep(tokio::time::Duration::from_secs(retry_delay));
                tokio::pin!(sleep_future);
                
                tokio::select! {
                    _ = &mut sleep_future => {
                        // Normal sleep completed
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                        if SHUTDOWN_FLAG.load(Ordering::SeqCst) {
                            println!("Shutdown signal received during retry wait, exiting...");
                            break;
                        }
                    }
                }
                
                // Exponential backoff with jitter
                let jitter = rand::random::<u64>() % 5;
                retry_delay = ((retry_delay * 2) + jitter).min(MAX_RETRY_DELAY);
            }
        }
    }

    println!("Shutdown complete.");
    Ok(())
}
