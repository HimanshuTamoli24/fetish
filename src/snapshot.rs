use crate::engine::Db;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const DEFAULT_SNAPSHOT_PATH: &str = "fetish_snapshot.json";
pub const SNAPSHOT_VERSION: u32 = 1;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct SnapshotFile {
    pub version: u32,
    pub created_at_epoch_ms: u64,
    pub entries: Vec<SnapshotEntry>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct SnapshotEntry {
    pub key: String,
    pub value: String,
    pub remaining_ttl_ms: Option<u64>,
}

pub struct SnapshotManager {
    path: PathBuf,
}

impl SnapshotManager {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Atomically saves a snapshot to disk using a temporary file and rename.
    /// Non-expired keys and their remaining TTL are preserved.
    pub fn save(&self, db: &Db) -> io::Result<()> {
        let entries_data = db.export_snapshot();
        let created_at_epoch_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;

        let entries = entries_data
            .into_iter()
            .map(|(key, value, remaining_ttl_ms)| SnapshotEntry {
                key,
                value,
                remaining_ttl_ms,
            })
            .collect();

        let snapshot = SnapshotFile {
            version: SNAPSHOT_VERSION,
            created_at_epoch_ms,
            entries,
        };

        let temp_path = self.path.with_extension(format!("tmp.{}", std::process::id()));

        // Ensure parent directory exists
        if let Some(parent) = temp_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }

        // Write atomically to temporary file first
        {
            let file = File::create(&temp_path)?;
            let mut writer = BufWriter::new(file);
            serde_json::to_writer_pretty(&mut writer, &snapshot)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        // Rename temporary file to target snapshot path atomically
        fs::rename(&temp_path, &self.path)?;
        Ok(())
    }

    /// Restores database state from a snapshot file.
    /// Returns Ok(count_of_restored_keys) or Err if snapshot is invalid.
    /// If snapshot file does not exist, returns Ok(0).
    /// Expired keys (whose TTL elapsed while Fetish was stopped) are not restored.
    pub fn restore(&self, db: &Db) -> io::Result<usize> {
        if !self.path.exists() {
            return Ok(0);
        }

        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let snapshot: SnapshotFile = serde_json::from_reader(reader)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("Corrupt or invalid snapshot: {}", e)))?;

        if snapshot.version != SNAPSHOT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported snapshot version: {}", snapshot.version),
            ));
        }

        let now_epoch_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;

        let elapsed_ms = now_epoch_ms.saturating_sub(snapshot.created_at_epoch_ms);

        let mut to_restore = Vec::new();

        for entry in snapshot.entries {
            match entry.remaining_ttl_ms {
                Some(ttl_ms) => {
                    if elapsed_ms < ttl_ms {
                        let remaining_ms = ttl_ms - elapsed_ms;
                        to_restore.push((entry.key, entry.value, Some(Duration::from_millis(remaining_ms))));
                    }
                }
                None => {
                    to_restore.push((entry.key, entry.value, None));
                }
            }
        }

        let count = to_restore.len();
        db.import_snapshot(to_restore);
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::thread;

    #[test]
    fn test_save_and_restore() {
        let snap_path = "test_snap_1.json";
        let _ = fs::remove_file(snap_path);

        let manager = SnapshotManager::new(snap_path);
        let db1 = Db::new();
        db1.s("name", "Alice");
        db1.s("city", "Delhi");

        assert!(manager.save(&db1).is_ok());

        let db2 = Db::new();
        let restored = manager.restore(&db2).unwrap();
        assert_eq!(restored, 2);
        assert_eq!(db2.g("name"), Some("Alice".to_string()));
        assert_eq!(db2.g("city"), Some("Delhi".to_string()));

        let _ = fs::remove_file(snap_path);
    }

    #[test]
    fn test_restore_with_ttl_and_expired() {
        let snap_path = "test_snap_ttl.json";
        let _ = fs::remove_file(snap_path);

        let manager = SnapshotManager::new(snap_path);
        let db1 = Db::new();
        db1.s("permanent", "value1");
        db1.set("alive_ttl", "value2", Some(10)).unwrap();
        db1.set("almost_expired", "value3", Some(1)).unwrap();

        assert!(manager.save(&db1).is_ok());

        // Wait 1.1s so almost_expired expires
        thread::sleep(Duration::from_millis(1100));

        let db2 = Db::new();
        let restored = manager.restore(&db2).unwrap();
        // almost_expired should not be restored
        assert_eq!(restored, 2);
        assert_eq!(db2.g("permanent"), Some("value1".to_string()));
        assert_eq!(db2.g("alive_ttl"), Some("value2".to_string()));
        assert_eq!(db2.g("almost_expired"), None);
        assert!(db2.ttl("alive_ttl") > 0);

        let _ = fs::remove_file(snap_path);
    }

    #[test]
    fn test_corrupt_snapshot_handling() {
        let snap_path = "test_snap_corrupt.json";
        {
            let mut f = File::create(snap_path).unwrap();
            f.write_all(b"{\"version\": 1, \"created_at_epoch_ms\": INVALID...").unwrap();
        }

        let manager = SnapshotManager::new(snap_path);
        let db = Db::new();
        assert!(manager.restore(&db).is_err());

        let _ = fs::remove_file(snap_path);
    }
}
