use crate::engine::Db;
use crate::resp::{execute_command, parse_resp_command};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub struct Server {
    addr: String,
    db: Db,
    running: Arc<AtomicBool>,
}

impl Server {
    pub fn new(addr: impl Into<String>, db: Db) -> Self {
        Self {
            addr: addr.into(),
            db,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn shutdown_handle(&self) -> Arc<AtomicBool> {
        self.running.clone()
    }

    pub fn run(&self) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.addr)?;
        listener.set_nonblocking(true)?;

        while self.running.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let db = self.db.clone();
                    let running = self.running.clone();
                    thread::spawn(move || {
                        handle_client(stream, db, running);
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }
}

fn handle_client(mut stream: TcpStream, db: Db, running: Arc<AtomicBool>) {
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let mut buffer = Vec::with_capacity(4096);
    let mut read_buf = [0u8; 4096];

    while running.load(Ordering::Relaxed) {
        match stream.read(&mut read_buf) {
            Ok(0) => {
                // Client cleanly disconnected
                break;
            }
            Ok(n) => {
                buffer.extend_from_slice(&read_buf[..n]);

                loop {
                    match parse_resp_command(&buffer) {
                        Ok(Some((cmd_parts, consumed))) => {
                            buffer.drain(..consumed);
                            if !cmd_parts.is_empty() {
                                let response = execute_command(&db, &cmd_parts);
                                if stream.write_all(response.as_bytes()).is_err() {
                                    return;
                                }
                                let _ = stream.flush();
                            }
                        }
                        Ok(None) => {
                            // Need more bytes to complete frame
                            break;
                        }
                        Err(err_msg) => {
                            let resp = format!("-ERR {}\r\n", err_msg);
                            let _ = stream.write_all(resp.as_bytes());
                            let _ = stream.flush();
                            buffer.clear();
                            break;
                        }
                    }
                }
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(_) => {
                // Disconnected or connection reset
                break;
            }
        }
    }
}
