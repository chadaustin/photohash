use anyhow::{anyhow, Context, Result};
use rusqlite::Connection;
use std::path::PathBuf;
use std::time::SystemTime;

use super::model;

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open() -> Result<Self> {
        let database_path = get_database_path()?;

        //eprintln!("opening database at {}\n", database_path.display());
        if let Some(parent) = database_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(&database_path)
            .with_context(|| format!("failed to open database at {}\n", database_path.display()))?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS files (
                path TEXT,
                inode INT,
                size INT,
                mtime INT,
                blake3 BLOB
            )",
            (),
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS images (
                blake3 BLOB,
                width INT,
                height INT,
                blockhash256 BLOB
            )",
            (),
        )?;

        Ok(Database { conn })
    }

    pub fn add_files(&self, files: &[model::ContentMetadata]) -> Result<()> {
        let mut query = self.conn.prepare(
            "INSERT INTO files (path, inode, size, mtime, blake3) VALUES (?, ?, ?, ?, ?)",
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
