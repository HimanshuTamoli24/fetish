use fetish::engine::Db;
use fetish::server::Server;
use fetish::snapshot::{SnapshotManager, DEFAULT_SNAPSHOT_PATH};
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    // Preserve V1 demonstration mode if requested or if --demo flag is passed
    if args.contains(&"--demo".to_string()) {
        run_demo();
        return;
    }

    let mut host = env::var("FETISH_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let mut port: u16 = env::var("FETISH_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(6379);
    let mut snapshot_path = env::var("FETISH_SNAPSHOT")
        .unwrap_or_else(|_| DEFAULT_SNAPSHOT_PATH.to_string());

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--host" => {
                if i + 1 < args.len() {
                    host = args[i + 1].clone();
                    i += 1;
                }
            }
            "--port" => {
                if i + 1 < args.len() {
                    if let Ok(p) = args[i + 1].parse() {
                        port = p;
                    }
                    i += 1;
                }
            }
            "--snapshot" => {
                if i + 1 < args.len() {
                    snapshot_path = args[i + 1].clone();
                    i += 1;
                }
            }
            "--help" | "-h" => {
                print_help();
                return;
            }
            _ => {}
        }
        i += 1;
    }

    let addr = format!("{}:{}", host, port);
    let db = Db::new();
    let snapshot_manager = SnapshotManager::new(&snapshot_path);

    // Restore snapshot if available
    match snapshot_manager.restore(&db) {
        Ok(count) if count > 0 => {
            println!("[Fetish] Restored {} keys from snapshot '{}'", count, snapshot_path);
        }
        Ok(_) => {
            println!("[Fetish] No existing snapshot loaded. Starting with empty store.");
        }
        Err(e) => {
            eprintln!("[Fetish] Warning: Failed to load snapshot ({}). Starting with empty store.", e);
        }
    }

    println!("[Fetish V2] In-memory database listening on {}", addr);
    println!("[Fetish V2] Supported commands: GET, SET [EX seconds], DEL, EXPIRE, TTL");

    let server = Server::new(&addr, db.clone());
    if let Err(e) = server.run() {
        eprintln!("[Fetish] Server error: {}", e);
    }
}

fn run_demo() {
    let db = Db::new();
    println!("s(\"name\", \"Himanshu\") -> {}", db.s("name", "Himanshu"));
    println!(
        "g(\"name\")             -> {}",
        match db.g("name") {
            Some(v) => format!("\"{}\"", v),
            None => "null".to_string(),
        }
    );
    // println!("d(\"name\")             -> {}", db.d("name"));
    println!(
        "g(\"name\")             -> {}",
        match db.g("name") {
            Some(v) => format!("\"{}\"", v),
            None => "null".to_string(),
        }
    );
}

fn print_help() {
    println!("Fetish V2 - In-Memory Database Server");
    println!("Usage:");
    println!("  fetish [OPTIONS]");
    println!();
    println!("Options:");
    println!("  --host <HOST>        Bind host (default: 127.0.0.1)");
    println!("  --port <PORT>        Bind port (default: 6379)");
    println!("  --snapshot <PATH>    Snapshot file path (default: fetish_snapshot.json)");
    println!("  --demo               Run V1 in-memory demonstration");
    println!("  --help, -h           Show this help message");
}
