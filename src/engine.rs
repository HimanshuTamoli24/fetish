use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct Entry {
    pub value: String,
    pub expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(value: String, expires_at: Option<Instant>) -> Self {
        Self { value, expires_at }
    }

    pub fn is_expired(&self, now: Instant) -> bool {
        match self.expires_at {
            Some(exp) => now >= exp,
            None => false,
        }
    }
}

/// Thread-safe in-memory key-value database engine with TTL support.
#[derive(Clone, Default, Debug)]
pub struct Db {
    data: Arc<RwLock<HashMap<String, Entry>>>,
}

impl Db {
    /// Creates a new empty database instance.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// `g(key)` — Get (Original V1 operation)
    /// Accept a key and return its stored value, or None if the key does not exist or has expired.
    pub fn g(&self, key: &str) -> Option<String> {
        self.get(key)
    }

    /// `s(key, value)` — Set (Original V1 operation)
    /// Accept a key and value, store in memory (overwriting if key exists, removing any TTL),
    /// and return a success response ("OK").
    pub fn s(&self, key: &str, value: &str) -> &'static str {
        let mut guard = self.data.write().expect("failed to acquire write lock");
        guard.insert(
            key.to_string(),
            Entry::new(value.to_string(), None),
        );
        "OK"
    }

    /// `d(key)` — Delete (Original V1 operation)
    /// Accept a key, delete key and value, and return whether the key existed and was deleted.
    /// Expired keys behave as nonexistent.
    pub fn d(&self, key: &str) -> bool {
        self.del(&[key]) > 0
    }

    /// `get(key)` — Redis-compatible GET
    /// Returns stored value, or None if missing or expired.
    pub fn get(&self, key: &str) -> Option<String> {
        let now = Instant::now();
        {
            let guard = self.data.read().expect("failed to acquire read lock");
            if let Some(entry) = guard.get(key) {
                if !entry.is_expired(now) {
                    return Some(entry.value.clone());
                }
            } else {
                return None;
            }
        }
        // Lazy expiration cleanup
        let mut guard = self.data.write().expect("failed to acquire write lock");
        if let Some(entry) = guard.get(key) {
            if entry.is_expired(now) {
                guard.remove(key);
            }
        }
        None
    }

    /// `set(key, value, ttl_secs)` — Redis-compatible SET with optional TTL
    /// Overwrites existing value and resets/applies TTL.
    pub fn set(&self, key: &str, value: &str, ttl_secs: Option<u64>) -> Result<&'static str, &'static str> {
        let expires_at = match ttl_secs {
            Some(secs) => {
                let duration = Duration::from_secs(secs);
                Some(Instant::now().checked_add(duration).ok_or("TTL overflow")?)
            }
            None => None,
        };

        let mut guard = self.data.write().expect("failed to acquire write lock");
        guard.insert(
            key.to_string(),
            Entry::new(value.to_string(), expires_at),
        );
        Ok("OK")
    }

    /// `del(keys)` — Redis-compatible DEL
    /// Deletes one or more keys. Returns the count of keys that were removed.
    /// Expired keys count as missing.
    pub fn del(&self, keys: &[&str]) -> usize {
        let now = Instant::now();
        let mut guard = self.data.write().expect("failed to acquire write lock");
        let mut deleted = 0;
        for &k in keys {
            if let Some(entry) = guard.get(k) {
                if !entry.is_expired(now) {
                    deleted += 1;
                }
            }
            guard.remove(k);
        }
        deleted
    }

    /// `expire(key, seconds)` — Redis-compatible EXPIRE
    /// Returns 1 if the timeout was set.
    /// Returns 0 if key does not exist or has expired.
    pub fn expire(&self, key: &str, seconds: i64) -> i64 {
        let now = Instant::now();
        let mut guard = self.data.write().expect("failed to acquire write lock");

        if let Some(entry) = guard.get_mut(key) {
            if entry.is_expired(now) {
                guard.remove(key);
                return 0;
            }

            if seconds <= 0 {
                // Non-positive TTL results in deletion
                guard.remove(key);
                return 1;
            }

            let duration = Duration::from_secs(seconds as u64);
            match now.checked_add(duration) {
                Some(exp) => {
                    entry.expires_at = Some(exp);
                    1
                }
                None => 0,
            }
        } else {
            0
        }
    }

    /// `ttl(key)` — Redis-compatible TTL
    /// Returns:
    /// - Remaining whole seconds for an expiring key.
    /// - -1 if the key exists but has no expiration.
    /// - -2 if the key does not exist or has expired.
    pub fn ttl(&self, key: &str) -> i64 {
        let now = Instant::now();
        {
            let guard = self.data.read().expect("failed to acquire read lock");
            if let Some(entry) = guard.get(key) {
                if !entry.is_expired(now) {
                    return match entry.expires_at {
                        Some(deadline) => {
                            let duration = deadline.saturating_duration_since(now);
                            let millis = duration.as_millis();
                            let secs = (millis + 999) / 1000;
                            secs as i64
                        }
                        None => -1,
                    };
                }
            } else {
                return -2;
            }
        }

        // Lazy cleanup if expired
        let mut guard = self.data.write().expect("failed to acquire write lock");
        if let Some(entry) = guard.get(key) {
            if entry.is_expired(now) {
                guard.remove(key);
            }
        }
        -2
    }

    /// Exports active, non-expired keys and their remaining TTL in milliseconds.
    pub fn export_snapshot(&self) -> Vec<(String, String, Option<u64>)> {
        let now = Instant::now();
        let guard = self.data.read().expect("failed to acquire read lock");
        let mut result = Vec::new();

        for (k, entry) in guard.iter() {
            if !entry.is_expired(now) {
                let remaining_ms = entry.expires_at.map(|exp| {
                    exp.saturating_duration_since(now).as_millis() as u64
                });
                result.push((k.clone(), entry.value.clone(), remaining_ms));
            }
        }
        result
    }

    /// Restores entries into the database.
    pub fn import_snapshot(&self, entries: Vec<(String, String, Option<Duration>)>) {
        let now = Instant::now();
        let mut guard = self.data.write().expect("failed to acquire write lock");
        guard.clear();

        for (k, v, remaining) in entries {
            let expires_at = remaining.map(|dur| now + dur);
            guard.insert(k, Entry::new(v, expires_at));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_set_and_get() {
        let db = Db::new();
        assert_eq!(db.s("name", "Himanshu"), "OK");
        assert_eq!(db.g("name"), Some("Himanshu".to_string()));
    }

    #[test]
    fn test_get_nonexistent_key() {
        let db = Db::new();
        assert_eq!(db.g("unknown"), None);
    }

    #[test]
    fn test_overwrite_existing_key() {
        let db = Db::new();
        db.s("key", "val1");
        assert_eq!(db.g("key"), Some("val1".to_string()));

        db.s("key", "val2");
        assert_eq!(db.g("key"), Some("val2".to_string()));
    }

    #[test]
    fn test_delete_existing_key() {
        let db = Db::new();
        db.s("key", "val");
        assert!(db.d("key"));
        assert_eq!(db.g("key"), None);
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let db = Db::new();
        assert!(!db.d("missing"));
    }

    #[test]
    fn test_example_flow() {
        let db = Db::new();
        assert_eq!(db.s("name", "Himanshu"), "OK");
        assert_eq!(db.g("name"), Some("Himanshu".to_string()));
        assert!(db.d("name"));
        assert_eq!(db.g("name"), None);
    }

    #[test]
    fn test_concurrent_access() {
        let db = Db::new();
        let mut handles = vec![];

        for i in 0..10 {
            let db_clone = db.clone();
            handles.push(thread::spawn(move || {
                let key = format!("k_{}", i);
                let val = format!("v_{}", i);
                assert_eq!(db_clone.s(&key, &val), "OK");
                assert_eq!(db_clone.g(&key), Some(val));
            }));
        }

        for handle in handles {
            handle.join().expect("thread panicked");
        }

        for i in 0..10 {
            let key = format!("k_{}", i);
            let val = format!("v_{}", i);
            assert_eq!(db.g(&key), Some(val));
        }
    }

    #[test]
    fn test_ttl_and_expiration() {
        let db = Db::new();
        // Set key with 1 second TTL
        assert!(db.set("temp", "value", Some(1)).is_ok());
        assert_eq!(db.get("temp"), Some("value".to_string()));
        assert_eq!(db.ttl("temp"), 1);

        // Sleep until expired
        thread::sleep(Duration::from_millis(1100));
        assert_eq!(db.get("temp"), None);
        assert_eq!(db.ttl("temp"), -2);
        assert_eq!(db.d("temp"), false);
    }

    #[test]
    fn test_ttl_values() {
        let db = Db::new();
        // Key does not exist: -2
        assert_eq!(db.ttl("missing"), -2);

        // Key exists without expiration: -1
        db.s("persistent", "data");
        assert_eq!(db.ttl("persistent"), -1);

        // Set expire
        assert_eq!(db.expire("persistent", 60), 1);
        assert!(db.ttl("persistent") <= 60 && db.ttl("persistent") >= 58);

        // Expire missing key returns 0
        assert_eq!(db.expire("nonexistent", 10), 0);
    }

    #[test]
    fn test_overwrite_removes_ttl() {
        let db = Db::new();
        db.set("key", "val1", Some(10)).unwrap();
        assert!(db.ttl("key") > 0);

        // Overwrite without TTL
        db.s("key", "val2");
        assert_eq!(db.ttl("key"), -1);
        assert_eq!(db.g("key"), Some("val2".to_string()));
    }

    #[test]
    fn test_expire_zero_or_negative_deletes() {
        let db = Db::new();
        db.s("key", "val");
        assert_eq!(db.expire("key", 0), 1);
        assert_eq!(db.g("key"), None);
        assert_eq!(db.ttl("key"), -2);
    }
}
