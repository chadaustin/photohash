use anyhow::{anyhow, Context, Result};
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;

use crate::ContentMetadata;
use crate::FileInfo;
use crate::Hash32;
use crate::ImageMetadata;

pub struct Database {
    conn: Mutex<Connection>,
}

impl Database {
    pub fn open() -> Result<Self> {
        let database_path = get_database_path()?;

        //eprintln!("opening database at {}", database_path.display());
        if let Some(parent) = database_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(&database_path)
            .with_context(|| format!("failed to open database at {}", database_path.display()))?;

        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;

        // TODO: on newer SQLite, use STRICT

        conn.execute(
            "CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY,
                inode INT NOT NULL,
                size INT NOT NULL,
                mtime INT NOT NULL,
                blake3 BLOB NOT NULL
            ) WITHOUT ROWID",
            (),
        )
        .context("failed to create `files` table")?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS images (
                blake3 BLOB PRIMARY KEY,
                width INT NOT NULL,
                height INT NOT NULL,
                blockhash256 BLOB NOT NULL
            ) WITHOUT ROWID",
            (),
        )
        .context("failed to create `images` table")?;

        Ok(Database {
            conn: Mutex::new(conn),
        })
    }

    pub fn add_files(&self, files: &[&ContentMetadata]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let mut query = conn.prepare(
            "INSERT OR REPLACE INTO files
            (path, inode, size, mtime, blake3)
            VALUES (?, ?, ?, ?, ?)",
        )?;
        for file in files {
            query.execute((
                // TODO: store paths as UTF-8
                &file.path.to_string_lossy(),
                &file.file_info.inode,
                &file.file_info.size,
                time_to_i64(&file.file_info.mtime),
                &file.blake3,
            ))?;
        }
        Ok(())
    }

    pub fn get_file<P: AsRef<Path>>(&self, path: P) -> Result<Option<ContentMetadata>> {
        let conn = self.conn.lock().unwrap();
        let mut query =
            conn.prepare("SELECT inode, size, mtime, blake3 FROM files WHERE path = ?")?;
        Ok(query
            .query_row((&path.as_ref().to_string_lossy(),), |row| {
                Ok(ContentMetadata {
                    path: path.as_ref().to_path_buf(),
                    file_info: FileInfo {
                        inode: row.get(0)?,
                        size: row.get(1)?,
                        mtime: i64_to_time(row.get(2)?),
                    },
                    blake3: row.get(3)?,
                })
            })
            .optional()?)
    }

    pub fn add_image_metadata(
        &self,
        blake3: &Hash32,
        image_metadata: &ImageMetadata,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let mut query = conn.prepare(
            "INSERT OR REPLACE INTO images
            (blake3, width, height, blockhash256)
            VALUES (?, ?, ?, ?)",
        )?;
        query.execute((
            &blake3,
            &image_metadata.image_width,
            &image_metadata.image_height,
            &image_metadata.blockhash256,
        ))?;
        Ok(())
    }

    pub fn get_image_metadata(&self, blake3: &Hash32) -> Result<Option<ImageMetadata>> {
        let conn = self.conn.lock().unwrap();
        let mut query =
            conn.prepare("SELECT width, height, blockhash256 FROM images WHERE blake3 = ?")?;
        Ok(query
            .query_row((&blake3,), |row| {
                Ok(ImageMetadata {
                    image_width: row.get(0)?,
                    image_height: row.get(1)?,
                    blockhash256: row.get(2)?,
                })
            })
            .optional()?)
    }
}

pub fn get_database_path() -> Result<PathBuf> {
    let dirs = match directories::BaseDirs::new() {
        Some(dirs) => dirs,
        None => {
            return Err(anyhow!("Failed to find local config directory"));
        }
    };
    let mut path = PathBuf::from(dirs.state_dir().unwrap_or(dirs.data_local_dir()));
    path.push("imagehash.sqlite");
    //eprintln!("the path is {}", path.display());
    Ok(path)
}

fn time_to_i64(time: &SystemTime) -> i64 {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i64,
        Err(e) => -(e.duration().as_nanos() as i64),
    }
}

fn i64_to_time(time: i64) -> SystemTime {
    if time >= 0 {
        SystemTime::UNIX_EPOCH + Duration::from_nanos(time as u64)
    } else {
        SystemTime::UNIX_EPOCH - Duration::from_nanos((-time) as u64)
    }
}

mod tests {
    use super::i64_to_time;
    use super::time_to_i64;
    use std::time::Duration;
    use std::time::SystemTime;

    #[test]
    fn epoch_is_zero() {
        assert_eq!(0, time_to_i64(&SystemTime::UNIX_EPOCH));
        assert_eq!(SystemTime::UNIX_EPOCH, i64_to_time(0));
    }

    #[test]
    fn ns_before_epoch() {
        // Windows only has 100 ns precision.
        let time = SystemTime::UNIX_EPOCH - Duration::from_nanos(100);
        assert_eq!(-100, time_to_i64(&time));
        assert_eq!(time, i64_to_time(-100));
    }
}
