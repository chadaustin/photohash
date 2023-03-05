use anyhow::{anyhow, Result};
use sqlite::Connection;
use std::path::PathBuf;

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open() -> Result<Self> {
        let database_path = get_database_path()?;

        eprintln!("opening database at {}\n", database_path.display());
        if let Some(parent) = database_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(database_path)?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS files (
                path TEXT,
                inode INT,
                size INT,
                mtime INT,
                blake3 BLOB
            )",
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS images (
                blake3 BLOB,
                width INT,
                height INT,
                blockhash256 BLOB
            )",
        )?;

        Ok(Database { conn })
    }

/*
    pub fn add_files(&self, files: &[model::FileInfo]) -> Result<()> {
        let query = "INSERT INTO files VALUES (:path, :inode, :size, :mtime, :blake3)";
        let mut stmt = self.conn.prepare(query)?;
        statement.bind_iter::<_, (_, Value)>([
            (":path", "Bob".into()),
            (":inode", 42.into()),
            (":size", )
        ])?;
    }
*/
}

fn get_database_path() -> Result<PathBuf> {
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
