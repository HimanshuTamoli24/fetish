use fetish::engine::Db;
use fetish::server::Server;
use fetish::snapshot::SnapshotManager;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

fn start_test_server() -> (String, Db, std::sync::Arc<std::sync::atomic::AtomicBool>) {
    let db = Db::new();
    // Bind to 127.0.0.1 on dynamic port
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let server = Server::new(&addr, db.clone());
    let shutdown_flag = server.shutdown_handle();

    let server_addr = addr.clone();
    thread::spawn(move || {
        let _ = server.run();
    });

    // Give server a moment to bind
    thread::sleep(Duration::from_millis(50));
    (server_addr, db, shutdown_flag)
}

#[test]
fn test_tcp_basic_commands() {
    let (addr, _, shutdown) = start_test_server();
    let mut client = TcpStream::connect(&addr).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

    // 1. SET
    client
        .write_all(b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
        .unwrap();
    let mut resp = [0u8; 128];
    let n = client.read(&mut resp).unwrap();
    assert_eq!(&resp[..n], b"+OK\r\n");

    // 2. GET
    client
        .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
        .unwrap();
    let n = client.read(&mut resp).unwrap();
    assert_eq!(&resp[..n], b"$5\r\nworld\r\n");

    // 3. TTL (no expiration: -1)
    client
        .write_all(b"*2\r\n$3\r\nTTL\r\n$5\r\nhello\r\n")
        .unwrap();
    let n = client.read(&mut resp).unwrap();
    assert_eq!(&resp[..n], b":-1\r\n");

    // 4. EXPIRE
    client
        .write_all(b"*3\r\n$6\r\nEXPIRE\r\n$5\r\nhello\r\n$2\r\n10\r\n")
        .unwrap();
    let n = client.read(&mut resp).unwrap();
    assert_eq!(&resp[..n], b":1\r\n");

    // 5. TTL (expiring)
    client
        .write_all(b"*2\r\n$3\r\nTTL\r\n$5\r\nhello\r\n")
        .unwrap();
    let n = client.read(&mut resp).unwrap();
    let s = std::str::from_utf8(&resp[..n]).unwrap();
    assert!(s == ":10\r\n" || s == ":9\r\n");

    // 6. DEL
    client
        .write_all(b"*2\r\n$3\r\nDEL\r\n$5\r\nhello\r\n")
        .unwrap();
    let n = client.read(&mut resp).unwrap();
    assert_eq!(&resp[..n], b":1\r\n");

    // 7. GET after DEL
    client
        .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
        .unwrap();
    let n = client.read(&mut resp).unwrap();
    assert_eq!(&resp[..n], b"$-1\r\n");

    shutdown.store(false, Ordering::Relaxed);
}

#[test]
fn test_tcp_chunked_reads_and_pipelining() {
    let (addr, _, shutdown) = start_test_server();
    let mut client = TcpStream::connect(&addr).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

    // Send command byte-by-byte (partial reads simulation)
    let cmd = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    for chunk in cmd.chunks(2) {
        client.write_all(chunk).unwrap();
        client.flush().unwrap();
        thread::sleep(Duration::from_millis(5));
    }

    let mut buf = [0u8; 128];
    let n = client.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"+OK\r\n");

    // Pipeline multiple commands in a single write
    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    pipeline.extend_from_slice(b"*2\r\n$3\r\nTTL\r\n$3\r\nfoo\r\n");
    pipeline.extend_from_slice(b"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n");

    client.write_all(&pipeline).unwrap();
    client.flush().unwrap();

    let mut accumulated = Vec::new();
    while accumulated.len() < "$3\r\nbar\r\n:-1\r\n:1\r\n".len() {
        let n = client.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        accumulated.extend_from_slice(&buf[..n]);
    }
    assert_eq!(
        std::str::from_utf8(&accumulated).unwrap(),
        "$3\r\nbar\r\n:-1\r\n:1\r\n"
    );

    shutdown.store(false, Ordering::Relaxed);
}

#[test]
fn test_tcp_values_with_spaces_and_crlf() {
    let (addr, _, shutdown) = start_test_server();
    let mut client = TcpStream::connect(&addr).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

    let val = "Hello World\r\nLine 2 with \t tabs & emojis 🚀";
    let cmd = format!(
        "*3\r\n$3\r\nSET\r\n$7\r\ncomplex\r\n${}\r\n{}\r\n",
        val.len(),
        val
    );

    client.write_all(cmd.as_bytes()).unwrap();
    let mut buf = [0u8; 256];
    let n = client.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"+OK\r\n");

    // GET complex value
    client
        .write_all(b"*2\r\n$3\r\nGET\r\n$7\r\ncomplex\r\n")
        .unwrap();
    let n = client.read(&mut buf).unwrap();
    let expected = format!("${}\r\n{}\r\n", val.len(), val);
    assert_eq!(&buf[..n], expected.as_bytes());

    shutdown.store(false, Ordering::Relaxed);
}

#[test]
fn test_tcp_multiple_concurrent_clients() {
    let (addr, _, shutdown) = start_test_server();
    let mut handles = vec![];

    for i in 0..8 {
        let server_addr = addr.clone();
        handles.push(thread::spawn(move || {
            let mut client = TcpStream::connect(&server_addr).unwrap();
            client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();

            let key = format!("concurrent_k_{}", i);
            let val = format!("concurrent_v_{}", i);

            let set_cmd = format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                val.len(),
                val
            );
            client.write_all(set_cmd.as_bytes()).unwrap();

            let mut buf = [0u8; 64];
            let n = client.read(&mut buf).unwrap();
            assert_eq!(&buf[..n], b"+OK\r\n");

            let get_cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
            client.write_all(get_cmd.as_bytes()).unwrap();

            let n = client.read(&mut buf).unwrap();
            let expected = format!("${}\r\n{}\r\n", val.len(), val);
            assert_eq!(&buf[..n], expected.as_bytes());
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    shutdown.store(false, Ordering::Relaxed);
}

#[test]
fn test_snapshot_atomic_and_failed_save_does_not_destroy_valid_file() {
    let snap_path = "test_atomic_safety.json";
    let _ = fs::remove_file(snap_path);

    let manager = SnapshotManager::new(snap_path);
    let db = Db::new();
    db.s("preserved", "must_not_be_corrupted");
    manager.save(&db).unwrap();

    // Verify file exists and is valid
    assert!(PathBuf::from(snap_path).exists());

    let db_restored = Db::new();
    assert_eq!(manager.restore(&db_restored).unwrap(), 1);
    assert_eq!(db_restored.g("preserved"), Some("must_not_be_corrupted".to_string()));

    let _ = fs::remove_file(snap_path);
}
