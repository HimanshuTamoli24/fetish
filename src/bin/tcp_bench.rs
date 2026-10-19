use fetish::engine::Db;
use fetish::server::Server;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

struct BenchConfig {
    name: &'static str,
    total_requests: usize,
    concurrency: usize,
    pipeline: usize,
}

#[allow(dead_code)]
struct BenchResult {
    name: &'static str,
    total_requests: usize,
    concurrency: usize,
    pipeline: usize,
    duration_secs: f64,
    ops_per_sec: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
}

fn main() {
    let addr = "127.0.0.1:6399";
    let db = Db::new();
    let server = Server::new(addr, db);
    let shutdown_flag = server.shutdown_handle();

    // Start server in background
    let s_thread = thread::spawn(move || {
        let _ = server.run();
    });
    thread::sleep(Duration::from_millis(100));

    println!("=================================================================================================");
    println!("                               Fetish V2 — TCP Benchmark Suite");
    println!(" Target: {} (Release build, multi-client concurrent TCP)", addr);
    println!("=================================================================================================\n");

    let configs = vec![
        BenchConfig {
            name: "SET (Single Client)",
            total_requests: 50_000,
            concurrency: 1,
            pipeline: 1,
        },
        BenchConfig {
            name: "GET (Single Client)",
            total_requests: 50_000,
            concurrency: 1,
            pipeline: 1,
        },
        BenchConfig {
            name: "SET (10 Clients Concurrent)",
            total_requests: 100_000,
            concurrency: 10,
            pipeline: 1,
        },
        BenchConfig {
            name: "GET (10 Clients Concurrent)",
            total_requests: 100_000,
            concurrency: 10,
            pipeline: 1,
        },
        BenchConfig {
            name: "SET (50 Clients Concurrent)",
            total_requests: 100_000,
            concurrency: 50,
            pipeline: 1,
        },
        BenchConfig {
            name: "GET (50 Clients Concurrent)",
            total_requests: 100_000,
            concurrency: 50,
            pipeline: 1,
        },
        BenchConfig {
            name: "SET (Pipelined P=16, 10 Clients)",
            total_requests: 100_000,
            concurrency: 10,
            pipeline: 16,
        },
        BenchConfig {
            name: "GET (Pipelined P=16, 10 Clients)",
            total_requests: 100_000,
            concurrency: 10,
            pipeline: 16,
        },
    ];

    let mut results = Vec::new();

    for cfg in configs {
        let res = run_workload(addr, &cfg);
        results.push(res);
    }

    shutdown_flag.store(false, Ordering::Relaxed);
    let _ = s_thread.join();

    println!("\n======================================================================================================");
    println!("| Workload                          | Requests | Clients | Pipeline | Ops/sec      | p50 (µs) | p99 (µs) |");
    println!("------------------------------------------------------------------------------------------------------");
    for r in &results {
        println!(
            "| {:<33} | {:>8} | {:>7} | {:>8} | {:>10.0} ops | {:>8.1} | {:>8.1} |",
            r.name,
            r.total_requests,
            r.concurrency,
            r.pipeline,
            r.ops_per_sec,
            r.p50_us,
            r.p99_us
        );
    }
    println!("======================================================================================================\n");
}

fn run_workload(addr: &str, cfg: &BenchConfig) -> BenchResult {
    let reqs_per_client = cfg.total_requests / cfg.concurrency;
    let actual_total = reqs_per_client * cfg.concurrency;
    let is_get = cfg.name.starts_with("GET");

    let latencies = Arc::new(std::sync::Mutex::new(Vec::with_capacity(actual_total)));
    let start_all = Instant::now();

    let mut handles = Vec::new();

    for c_id in 0..cfg.concurrency {
        let latencies_clone = latencies.clone();
        let target_addr = addr.to_string();
        let pipeline = cfg.pipeline;

        handles.push(thread::spawn(move || {
            let mut client = TcpStream::connect(target_addr).expect("connect failed");
            client.set_read_timeout(Some(Duration::from_secs(10))).expect("timeout failed");
            let mut local_lats = Vec::with_capacity(reqs_per_client);
            let mut buf = [0u8; 16384];

            let mut remaining = reqs_per_client;
            while remaining > 0 {
                let batch = remaining.min(pipeline);
                let mut cmd_bytes = Vec::new();

                for i in 0..batch {
                    let key = format!("k_{}_{}", c_id, i);
                    if is_get {
                        let cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
                        cmd_bytes.extend_from_slice(cmd.as_bytes());
                    } else {
                        let val = format!("val_{}", i);
                        let cmd = format!(
                            "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                            key.len(),
                            key,
                            val.len(),
                            val
                        );
                        cmd_bytes.extend_from_slice(cmd.as_bytes());
                    }
                }

                let t0 = Instant::now();
                client.write_all(&cmd_bytes).expect("write failed");
                client.flush().expect("flush failed");

                // Count completed responses
                let mut responses_read = 0;
                while responses_read < batch {
                    let n = client.read(&mut buf).expect("read failed");
                    if n == 0 {
                        break;
                    }
                    if is_get {
                        // GET responses have 2 newlines ($len\r\nval\r\n)
                        let newlines = buf[..n].iter().filter(|&&b| b == b'\n').count();
                        responses_read += newlines / 2;
                    } else {
                        // SET responses have 1 newline (+OK\r\n)
                        let newlines = buf[..n].iter().filter(|&&b| b == b'\n').count();
                        responses_read += newlines;
                    }
                }

                let elapsed_us = t0.elapsed().as_nanos() as f64 / 1_000.0 / (batch as f64);
                for _ in 0..batch {
                    local_lats.push(elapsed_us);
                }

                remaining -= batch;
            }

            let mut guard = latencies_clone.lock().unwrap();
            guard.extend(local_lats);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let total_duration = start_all.elapsed().as_secs_f64();
    let mut all_lats = latencies.lock().unwrap().clone();
    all_lats.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let p50 = all_lats.get(all_lats.len() * 50 / 100).copied().unwrap_or(0.0);
    let p95 = all_lats.get(all_lats.len() * 95 / 100).copied().unwrap_or(0.0);
    let p99 = all_lats.get(all_lats.len() * 99 / 100).copied().unwrap_or(0.0);
    let ops_per_sec = actual_total as f64 / total_duration;

    print!(".");
    let _ = std::io::stdout().flush();

    BenchResult {
        name: cfg.name,
        total_requests: actual_total,
        concurrency: cfg.concurrency,
        pipeline: cfg.pipeline,
        duration_secs: total_duration,
        ops_per_sec,
        p50_us: p50,
        p95_us: p95,
        p99_us: p99,
    }
}
