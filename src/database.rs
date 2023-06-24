use anyhow::{anyhow, Context, Result};
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use rusqlite::Statement;
use self_cell::self_cell;
use std::collections::HashMap;
use std::convert::TryInto;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;

use crate::model::ContentMetadata;
use crate::model::FileInfo;
use crate::model::Hash32;
use crate::model::ImageMetadata;

pub struct CachedStatements<'conn> {
    begin_tx: Statement<'conn>,
    commit_tx: Statement<'conn>,
    rollback_tx: Statement<'conn>,
    get_file: Statement<'conn>,
    get_files_10: Statement<'conn>,
}

unsafe impl Send for CachedStatements<'_> {}

fn cache_statements(conn: &Connection) -> CachedStatements<'_> {
    CachedStatements {
        begin_tx: conn.prepare("BEGIN TRANSACTION").unwrap(),
        commit_tx: conn.prepare("COMMIT").unwrap(),
        rollback_tx: conn.prepare("ROLLBACK").unwrap(),
        // TODO: We could elide `path` from the singular case because we already know it.
        get_file: conn
            .prepare("SELECT inode, size, mtime, blake3 FROM files WHERE path = ?")
            .unwrap(),
        get_files_10: conn
            .prepare("SELECT inode, size, mtime, blake3, path FROM files WHERE path IN (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .unwrap(),
    }
}

self_cell!(
    struct DatabaseInner {
        owner: Connection,

        #[covariant]
        dependent: CachedStatements,
    }
);

pub struct Database(DatabaseInner);

impl Database {
    fn conn(&self) -> &Connection {
        self.0.borrow_owner()
    }

    fn conn_mut(&mut self) -> &Connection {
        self.0.borrow_owner()
    }
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

        Self::init_schema(conn)
    }

    pub fn open_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;

        Self::init_schema(conn)
    }

    fn init_schema(conn: Connection) -> Result<Self> {
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

        Ok(Database(DatabaseInner::new(conn, cache_statements)))
    }

    /// For benchmarking.
    pub fn rusqlite_transaction(&mut self) -> Result<()> {
        self.conn().unchecked_transaction()?;
        Ok(())
    }

    pub fn cached_immediate_transaction(&self) -> Result<()> {
        let _ = self
            .conn()
            .prepare_cached("BEGIN IMMEDIATE TRANSACTION")?
            .execute(())?;
        let _ = self.conn().prepare_cached("COMMIT")?.execute(())?;
        Ok(())
    }

    pub fn cached_deferred_transaction(&self) -> Result<()> {
        let _ = self
            .conn()
            .prepare_cached("BEGIN DEFERRED TRANSACTION")?
            .execute(())?;
        let _ = self.conn().prepare_cached("COMMIT")?.execute(())?;
        Ok(())
    }

    pub fn with_transaction<T, F>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection, &mut CachedStatements) -> Result<T>,
    {
        self.0.with_dependent_mut(|conn, stmt| {
            let _ = stmt.begin_tx.execute(())?;
            let result = f(conn, stmt);
            match &result {
                Ok(_) => {
                    stmt.commit_tx.execute(())?;
                }
                Err(_) => {
                    stmt.rollback_tx.execute(())?;
                }
            }
            result
        })
    }

    fn file_from_single_row(row: &rusqlite::Row) -> rusqlite::Result<ContentMetadata> {
        Ok(ContentMetadata {
            file_info: FileInfo {
                inode: row.get(0)?,
                size: row.get(1)?,
                mtime: i64_to_time(row.get(2)?),
            },
            blake3: row.get(3)?,
        })
    }

    pub fn get_file(&mut self, path: &str) -> Result<Option<ContentMetadata>> {
        self.0.with_dependent_mut(|conn, stmt| {
            Ok(stmt
                .get_file
                .query_row((path,), Self::file_from_single_row)
                .optional()?)
        })
    }

    pub fn get_files<PS, P>(&mut self, paths: PS) -> Result<Vec<Option<ContentMetadata>>>
    where
        PS: AsRef<[P]>,
        P: AsRef<str> + Copy,
    {
        const N: usize = 10;

        self.with_transaction(|conn, stmt| {
            let paths = paths.as_ref();
            let mut results = Vec::with_capacity(paths.len());

            let mut chunks = paths.iter().map(|path| path.as_ref()).array_chunks::<N>();
            for chunk in chunks.by_ref() {
                let i = results.len();
                results.resize_with(results.len() + N, || None);

                let mut rows = stmt.get_files_10.query(chunk)?;
                while let Some(row) = rows.next()? {
                    let path_ref = row.get_ref(4)?;
                    for j in i..(i + N) {
                        if paths[j].as_ref() == path_ref.as_str()? {
                            results[j] = Some(Self::file_from_single_row(row)?);
                            break;
                        }
                    }
                }
            }

            // TODO: Add bulk select variants for all chunk sizes.
            if let Some(remainder) = chunks.into_remainder() {
                for path in remainder {
                    results.push(
                        stmt.get_file
                            .query_row((path,), Self::file_from_single_row)
                            .optional()?,
                    );
                }
            }

            assert_eq!(results.len(), paths.len());
            Ok(results)
        })
    }

    pub fn add_files(&self, files: &[(&str, &ContentMetadata)]) -> Result<()> {
        let mut query = self.conn().prepare_cached(
            "INSERT OR REPLACE INTO files
            (path, inode, size, mtime, blake3)
            VALUES (?, ?, ?, ?, ?)",
        )?;
        for (path, file) in files {
            query.execute((
                path,
                &file.file_info.inode,
                &file.file_info.size,
                time_to_i64(&file.file_info.mtime),
                &file.blake3,
            ))?;
        }
        Ok(())
    }

    pub fn add_image_metadata(
        &self,
        blake3: &Hash32,
        image_metadata: &ImageMetadata,
    ) -> Result<()> {
        let mut query = self.conn().prepare_cached(
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
        let mut query = self
            .conn()
            .prepare("SELECT width, height, blockhash256 FROM images WHERE blake3 = ?")?;
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
    use super::*;
    use anyhow::Error;
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

    #[test]
    fn in_memory_database() -> Result<()> {
        let mut db = Database::open_memory()?;

        let path = String::from("test");

        assert_eq!(None, db.get_file(&path)?);

        let cm = ContentMetadata {
            file_info: FileInfo {
                inode: 10,
                size: 20,
                mtime: SystemTime::UNIX_EPOCH,
            },
            blake3: blake3::hash(b"foo").into(),
        };
        db.add_files(&[(&path, &cm)])?;

        let result = db.get_file(&path)?.unwrap();
        assert_eq!(10, result.file_info.inode);

        Ok(())
    }
}
