use crate::config;
use crate::flops::measure_flops;
use crate::memory_stats::get_memory_info;
use crate::nexus_orchestrator::{
    GetProofTaskRequest, GetProofTaskResponse, NodeType, SubmitProofRequest,
};
use prost::Message;
use reqwest::{Client, ClientBuilder};
use tokio::time::{sleep, Duration};
use rand::Rng;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};
use std::collections::VecDeque;
use futures::{self, future::select_ok, FutureExt};
use std::error::Error;

// Define our own Error type
#[derive(Debug)]
pub struct OrchestratorError(String);

impl std::fmt::Display for OrchestratorError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for OrchestratorError {}

type Result<T> = std::result::Result<T, OrchestratorError>;

// Move NoTaskBackoff outside impl block
struct NoTaskBackoff {
    current_delay: u64,
    consecutive_no_tasks: u32,
}

impl NoTaskBackoff {
    const NO_TASK_BACKOFF_INITIAL_MS: u64 = 5000;  // 5 seconds
    const NO_TASK_BACKOFF_MAX_MS: u64 = 300000;    // 5 minutes
    const NO_TASK_BACKOFF_MULTIPLIER: f64 = 1.5;
}

impl Default for NoTaskBackoff {
    fn default() -> Self {
        Self {
            current_delay: Self::NO_TASK_BACKOFF_INITIAL_MS,
            consecutive_no_tasks: 0,
        }
    }
}

pub struct OrchestratorClient {
    client: Client,
    base_url: String,
    task_cache: Arc<Mutex<VecDeque<GetProofTaskResponse>>>,
    prefetch_threshold: usize,
}

impl OrchestratorClient {
    const MAX_RETRIES: u32 = 5;
    const INITIAL_RETRY_DELAY_MS: u64 = 1000;
    const MAX_RETRY_DELAY_MS: u64 = 32000;
    const CACHE_SIZE: usize = 5;
    const PREFETCH_THRESHOLD: usize = 2;
    const CONNECT_TIMEOUT_SECS: u64 = 10;
    const REQUEST_TIMEOUT_SECS: u64 = 30;
    const MAX_CONNECTIONS: usize = 100;
    const POOL_IDLE_TIMEOUT_SECS: u64 = 90;
    const PARALLEL_REQUESTS: usize = 2000; // Number of parallel requests to make

    pub fn new(environment: config::Environment) -> Self {
        let client = ClientBuilder::new()
            .connect_timeout(Duration::from_secs(Self::CONNECT_TIMEOUT_SECS))
            .timeout(Duration::from_secs(Self::REQUEST_TIMEOUT_SECS))
            .pool_max_idle_per_host(Self::MAX_CONNECTIONS)
            .pool_idle_timeout(Duration::from_secs(Self::POOL_IDLE_TIMEOUT_SECS))
            .tcp_keepalive(Duration::from_secs(60))
            .tcp_nodelay(true)
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            client,
            base_url: environment.orchestrator_url(),
            task_cache: Arc::new(Mutex::new(VecDeque::with_capacity(Self::CACHE_SIZE))),
            prefetch_threshold: Self::PREFETCH_THRESHOLD,
        }
    }

    async fn make_request<T, U>(
        &self,
        url: &str,
        method: &str,
        request_data: &T,
    ) -> Result<Option<U>>
    where
        T: Message,
        U: Message + Default,
    {
        let request_bytes = request_data.encode_to_vec();
        let url = format!("{}{}", self.base_url, url);

        let response = match method {
            "POST" => self.client.post(&url)
                .header("Content-Type", "application/octet-stream")
                .body(request_bytes)
                .send()
                .await
                .map_err(|e| OrchestratorError(format!("Request failed: {}", e)))?,
            "GET" => self.client.get(&url)
                .send()
                .await
                .map_err(|e| OrchestratorError(format!("Request failed: {}", e)))?,
            _ => return Err(OrchestratorError("Unsupported HTTP method".to_string())),
        };

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await
                .unwrap_or_else(|_| "Unknown error".to_string());

            let error_msg = match status.as_u16() {
                400 => format!("[400] Invalid request: {}", error_text),
                401 => format!("[401] Authentication failed: {}", error_text),
                403 => format!("[403] Permission denied: {}", error_text),
                404 => format!("[404] Resource not found: {}", error_text),
                429 => format!("[429] Too many requests: {}", error_text),
                500..=599 => format!("[{}] Server error: {}", status, error_text),
                _ => format!("[{}] Unexpected error: {}", status, error_text),
            };

            return Err(OrchestratorError(error_msg));
        }

        let response_bytes = response.bytes().await
            .map_err(|e| OrchestratorError(format!("Failed to read response: {}", e)))?;

        if response_bytes.is_empty() {
            return Ok(None);
        }

        U::decode(response_bytes)
            .map(Some)
            .map_err(|e| OrchestratorError(format!("Failed to decode response: {}", e)))
    }

    async fn make_request_with_retry<T, U>(
        &self,
        url: &str,
        method: &str,
        request_data: &T,
    ) -> Result<Option<U>>
    where
        T: Message + Clone,
        U: Message + Default,
    {
        let mut retries = 0;
        let mut delay_ms = Self::INITIAL_RETRY_DELAY_MS;

        loop {
            match self.make_request(url, method, request_data).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    if retries >= Self::MAX_RETRIES {
                        return Err(e);
                    }

                    let jitter: i32 = rand::thread_rng().gen_range(-100..100);
                    let delay = Duration::from_millis(delay_ms.saturating_add(jitter.abs() as u64));
                    
                    println!("Request failed, retrying in {} ms: {}", delay.as_millis(), e);
                    sleep(delay).await;

                    retries += 1;
                    delay_ms = (delay_ms * 2).min(Self::MAX_RETRY_DELAY_MS);
                }
            }
        }
    }

    async fn fetch_tasks_batch(
        &self,
        node_id: &str,
        batch_size: usize,
    ) -> Result<Vec<GetProofTaskResponse>> {
        let mut tasks = Vec::with_capacity(batch_size);
        let mut successful_fetches = 0;
        let mut consecutive_failures = 0;
        const MAX_CONSECUTIVE_FAILURES: usize = 3;

        while tasks.len() < batch_size && consecutive_failures < MAX_CONSECUTIVE_FAILURES {
            let remaining = batch_size - tasks.len();
            let request_count = remaining.min(5);
            let mut futures = Vec::with_capacity(request_count);

            // Create futures for each request
            for _ in 0..request_count {
                let request = GetProofTaskRequest {
                    node_id: node_id.to_string(),
                    node_type: NodeType::CliProver as i32,
                };
                
                // Clone the request for the future
                let request_clone = request.clone();
                let future = async move {
                    self.make_request_with_retry::<GetProofTaskRequest, GetProofTaskResponse>(
                        "/tasks", 
                        "POST", 
                        &request_clone
                    ).await
                };
                futures.push(future);
            }

            let results = futures::future::join_all(futures).await;
            let mut batch_success = false;
            
            for result in results {
                match result {
                    Ok(Some(response)) if !response.program_id.is_empty() => {
                        tasks.push(response);
                        successful_fetches += 1;
                        batch_success = true;
                        consecutive_failures = 0;
                    }
                    Ok(_) => {
                        if !batch_success {
                            consecutive_failures += 1;
                        }
                    }
                    Err(e) => {
                        println!("Failed to fetch task in batch: {}", e);
                        if !batch_success {
                            consecutive_failures += 1;
                        }
                    }
                }
            }

            if batch_success {
                if successful_fetches >= 2 {
                    continue;
                }
                sleep(Duration::from_millis(100)).await;
            } else {
                sleep(Duration::from_secs(1)).await;
            }
        }

        Ok(tasks)
    }

    async fn prefetch_tasks(&self, node_id: &str) {
        let cache = self.task_cache.clone();
        let node_id = node_id.to_string();
        let batch_size = Self::CACHE_SIZE * 2;
        let client = self.client.clone();
        let base_url = self.base_url.clone();
        
        tokio::spawn(async move {
            let spawned_client = OrchestratorClient {
                client,
                base_url,
                task_cache: cache.clone(),
                prefetch_threshold: Self::PREFETCH_THRESHOLD,
            };

            match spawned_client.fetch_tasks_batch(&node_id, batch_size).await {
                Ok(tasks) => {
                    let task_count = tasks.len();
                    let mut cache = cache.lock().await;
                    for task in tasks {
                        if cache.len() < Self::CACHE_SIZE {
                            cache.push_back(task);
                        }
                    }
                    if task_count > 0 {
                        println!("Successfully prefetched {} tasks", task_count);
                    }
                }
                Err(e) => println!("Failed to prefetch tasks: {}", e),
            }
        });
    }

    async fn fetch_single_task(
        &self,
        node_id: &str,
    ) -> Result<Option<GetProofTaskResponse>> {
        let request = GetProofTaskRequest {
            node_id: node_id.to_string(),
            node_type: NodeType::CliProver as i32,
        };

        let response: Option<GetProofTaskResponse> = self
            .make_request_with_retry("/tasks", "POST", &request)
            .await?;

        if let Some(ref task) = response {
            if task.program_id.is_empty() {
                println!("No tasks currently available from orchestrator");
                return Ok(None);
            }
        }

        Ok(response)
    }

    async fn fetch_parallel_tasks(
        &self,
        node_id: &str,
    ) -> Result<GetProofTaskResponse> {
        let (cancel_tx, _) = broadcast::channel(1);
        let mut futures = Vec::with_capacity(Self::PARALLEL_REQUESTS);

        for _ in 0..Self::PARALLEL_REQUESTS {
            let request = GetProofTaskRequest {
                node_id: node_id.to_string(),
                node_type: NodeType::CliProver as i32,
            };
            let mut cancel_rx = cancel_tx.subscribe();
            let client = self.clone();

            let future = async move {
                tokio::select! {
                    result = client.fetch_single_task(&request.node_id) => {
                        match result? {
                            Some(task) if !task.program_id.is_empty() => Ok(task),
                            _ => Err(OrchestratorError("No task available".to_string())),
                        }
                    }
                    _ = cancel_rx.recv() => {
                        Err(OrchestratorError("Request cancelled".to_string()))
                    }
                }
            }.boxed();

            futures.push(future);
        }

        let result = select_ok(futures).await;
        // Cancel all other requests
        let _ = cancel_tx.send(());

        match result {
            Ok((task, _remaining)) => Ok(task),
            Err(e) => Err(OrchestratorError(format!("All parallel requests failed: {}", e)))
        }
    }

    pub async fn get_proof_task(
        &self,
        node_id: &str,
    ) -> Result<GetProofTaskResponse> {
        let mut cache = self.task_cache.lock().await;
        
        if let Some(task) = cache.pop_front() {
            if cache.len() <= self.prefetch_threshold {
                drop(cache);
                self.prefetch_tasks(node_id).await;
            }
            return Ok(task);
        }
        drop(cache);

        let mut backoff = NoTaskBackoff::default();
        
        loop {
            match self.fetch_parallel_tasks(node_id).await {
                Ok(task) => {
                    self.prefetch_tasks(node_id).await;
                    return Ok(task);
                }
                Err(_) => {
                    backoff.consecutive_no_tasks += 1;
                    
                    let jitter: i32 = rand::thread_rng().gen_range(-500..500);
                    let delay = Duration::from_millis(
                        backoff.current_delay.saturating_add(jitter.abs() as u64)
                    );
                    
                    println!(
                        "No tasks available. Waiting {} seconds before retry (attempt {})",
                        delay.as_secs_f64(),
                        backoff.consecutive_no_tasks
                    );
                    
                    sleep(delay).await;
                    
                    backoff.current_delay = (
                        (backoff.current_delay as f64 * NoTaskBackoff::NO_TASK_BACKOFF_MULTIPLIER) as u64
                    ).min(NoTaskBackoff::NO_TASK_BACKOFF_MAX_MS);
                }
            }
        }
    }

    pub async fn submit_proof(
        &self,
        node_id: &str,
        proof_hash: &str,
        proof: Vec<u8>,
    ) -> Result<()> {
        let (program_memory, total_memory) = get_memory_info();
        let flops = measure_flops();

        let request = SubmitProofRequest {
            node_id: node_id.to_string(),
            node_type: NodeType::CliProver as i32,
            proof_hash: proof_hash.to_string(),
            proof,
            node_telemetry: Some(crate::nexus_orchestrator::NodeTelemetry {
                flops_per_sec: Some(flops as i32),
                memory_used: Some(program_memory),
                memory_capacity: Some(total_memory),
                location: Some("US".to_string()),
            }),
        };

        self.make_request::<SubmitProofRequest, ()>("/tasks/submit", "POST", &request)
            .await?;

        Ok(())
    }
}
