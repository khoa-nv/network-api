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
use tokio::sync::Mutex;
use std::collections::VecDeque;
use futures;
use std::error::Error as StdError;

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
    // environment: config::Environment,
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
            // environment,
        }
    }

    async fn make_request<T, U>(
        &self,
        url: &str,
        method: &str,
        request_data: &T,
    ) -> Result<Option<U>, Box<dyn StdError + Send + Sync>>
    where
        T: Message,
        U: Message + Default,
    {
        let request_bytes = request_data.encode_to_vec();
        let url = format!("{}{}", self.base_url, url);

        let friendly_connection_error =
            "[CONNECTION] Unable to reach server. The service might be temporarily unavailable."
                .to_string();
        let friendly_messages = match method {
            "POST" => match self
                .client
                .post(&url)
                .header("Content-Type", "application/octet-stream")
                .body(request_bytes)
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(_) => return Err(friendly_connection_error.into()),
            },
            "GET" => match self.client.get(&url).send().await {
                Ok(resp) => resp,
                Err(_) => return Err(friendly_connection_error.into()),
            },
            _ => return Err("[METHOD] Unsupported HTTP method".into()),
        };

        if !friendly_messages.status().is_success() {
            let status = friendly_messages.status();
            let error_text = friendly_messages.text().await?;

            // Clean up error text by removing HTML
            let clean_error = if error_text.contains("<html>") {
                format!("HTTP {}", status.as_u16())
            } else {
                error_text
            };

            let friendly_message = match status.as_u16() {
                400 => "[400] Invalid request".to_string(),
                401 => "[401] Authentication failed. Please check your credentials.".to_string(),
                403 => "[403] You don't have permission to perform this action.".to_string(),
                404 => "[404] The requested resource was not found.".to_string(),
                408 => "[408] The server timed out waiting for your request. Please try again.".to_string(),
                429 => "[429] Too many requests. Please try again later.".to_string(),
                502 => "[502] Unable to reach the server. Please try again later.".to_string(),
                504 => "[504] Gateway Timeout: The server took too long to respond. Please try again later.".to_string(),
                500..=599 => format!("[{}] A server error occurred. Our team has been notified. Please try again later.", status),
                _ => format!("[{}] Unexpected error: {}", status, clean_error),
            };

            return Err(friendly_message.into());
        }

        let response_bytes = friendly_messages.bytes().await?;
        if response_bytes.is_empty() {
            return Ok(None);
        }

        match U::decode(response_bytes) {
            Ok(msg) => Ok(Some(msg)),
            Err(_e) => {
                // println!("Failed to decode response: {:?}", e);
                Ok(None)
            }
        }
    }

    async fn make_request_with_retry<T, U>(
        &self,
        url: &str,
        method: &str,
        request_data: &T,
    ) -> Result<Option<U>, Box<dyn StdError + Send + Sync>>
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

                    // Add jitter to prevent thundering herd
                    let jitter = rand::thread_rng().gen_range(-100..100);
                    let delay = Duration::from_millis(delay_ms.saturating_add(jitter as u64));
                    
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
    ) -> Result<Vec<GetProofTaskResponse>, Box<dyn StdError + Send + Sync>> {
        let mut tasks = Vec::with_capacity(batch_size);
        let mut successful_fetches = 0;
        let mut consecutive_failures = 0;
        const MAX_CONSECUTIVE_FAILURES: usize = 3;

        while tasks.len() < batch_size && consecutive_failures < MAX_CONSECUTIVE_FAILURES {
            let remaining = batch_size - tasks.len();
            let request_count = remaining.min(5);
            let mut futures = Vec::with_capacity(request_count);

            for _ in 0..request_count {
                let request = GetProofTaskRequest {
                    node_id: node_id.to_string(),
                    node_type: NodeType::CliProver as i32,
                };
                futures.push(self.make_request_with_retry::<GetProofTaskRequest, GetProofTaskResponse>("/tasks", "POST", &request));
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
    ) -> Result<Option<GetProofTaskResponse>, Box<dyn StdError + Send + Sync>> {
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

    pub async fn get_proof_task(
        &self,
        node_id: &str,
    ) -> Result<GetProofTaskResponse, Box<dyn StdError + Send + Sync>> {
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
            match self.fetch_single_task(node_id).await? {
                Some(task) => {
                    self.prefetch_tasks(node_id).await;
                    return Ok(task);
                }
                None => {
                    backoff.consecutive_no_tasks += 1;
                    
                    let jitter = rand::thread_rng().gen_range(-500i32..500i32) as i64;
                    let delay = Duration::from_millis(
                        backoff.current_delay.saturating_add(jitter.unsigned_abs() as u64)
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
    ) -> Result<(), Box<dyn StdError + Send + Sync>> {
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
