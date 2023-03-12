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
                // TODO: store mtime as i64 since epoch -- negative is okay
                file.file_info
                    .mtime
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64,
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
                        mtime: SystemTime::UNIX_EPOCH + Duration::from_nanos(row.get(2)?),
                    },
                    blake3: row.get(3)?,
                })
            })
            .optional()?)
    }

    /*
        pub fn add_files(&self, files: &[model::FileInfo]) -> Result<()> {
            statement.bind_iter::<_, (_, Value)>([
                (":path", "Bob".into()),
                (":inode", 42.into()),
                (":size", )
            ])?;
        }
    */
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
