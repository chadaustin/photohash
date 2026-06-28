#[cfg(not(unix))]
use anyhow::anyhow;
use anyhow::Context;
use clap::Args;
use clap::Subcommand;
use photohash::database;
use photohash::model::Hash32;
use photohash::Database;
#[cfg(not(windows))]
use std::os::unix::process::CommandExt;
use std::process::Command;

#[derive(Debug, Args)]
#[command(name = "path", about = "Print database location")]
pub struct DbPath {}

impl DbPath {
    async fn run(&self) -> anyhow::Result<()> {
        println!("{}", database::get_database_path()?.display());
        Ok(())
    }
}

#[derive(Debug, Args)]
#[command(name = "open", about = "Interactively explore database")]
pub struct DbOpen {}

impl DbOpen {
    async fn run(&self) -> anyhow::Result<()> {
        let mut cmd = Command::new("sqlite3");
        cmd.arg(database::get_database_path()?);
        Self::exec(cmd).context("failed to run sqlite3")
    }

    #[cfg(unix)]
    fn exec(mut cmd: Command) -> anyhow::Result<()> {
        Err(cmd.exec().into())
    }

    #[cfg(not(unix))]
    fn exec(mut cmd: Command) -> anyhow::Result<()> {
        let status = cmd.status()?;
        if status.success() {
            Ok(())
        } else {
            Err(anyhow!("Failed to run sqlite3 command"))
        }
    }
}

#[derive(Debug, Args)]
#[command(
    name = "minunique",
    about = "Print the minimum hex prefix length that uniquely identifies image BLAKE3 hashes"
)]
pub struct DbMinUnique {}

impl DbMinUnique {
    async fn run(&self) -> anyhow::Result<()> {
        let db = Database::open()?;
        let mut min_unique = MinUniqueHexChars::default();
        db.for_each_image_blake3_hash(|hash| {
            min_unique.add_sorted(hash);
            Ok(())
        })?;
        println!("{}", min_unique.finish());
        Ok(())
    }
}

#[derive(Default)]
struct MinUniqueHexChars {
    previous: Option<Hash32>,
    hashes: usize,
    max_common_prefix: usize,
}

impl MinUniqueHexChars {
    fn add_sorted(&mut self, hash: Hash32) {
        if self.previous == Some(hash) {
            return;
        }

        if let Some(previous) = self.previous {
            self.max_common_prefix = self
                .max_common_prefix
                .max(common_hex_prefix_len(&previous, &hash));
        }

        self.previous = Some(hash);
        self.hashes += 1;
    }

    fn finish(self) -> usize {
        match self.hashes {
            0 => 0,
            1 => 1,
            _ => self.max_common_prefix + 1,
        }
    }
}

fn common_hex_prefix_len(a: &Hash32, b: &Hash32) -> usize {
    let mut nibbles = 0;
    for (a, b) in a.iter().zip(b) {
        if a >> 4 != b >> 4 {
            return nibbles;
        }
        nibbles += 1;

        if a & 0x0f != b & 0x0f {
            return nibbles;
        }
        nibbles += 1;
    }
    nibbles
}

#[derive(Debug, Subcommand)]
#[command(name = "db", about = "Database administration")]
pub enum Db {
    #[command(name = "path")]
    DbPath(DbPath),
    #[command(name = "open")]
    DbOpen(DbOpen),
    #[command(name = "minunique")]
    DbMinUnique(DbMinUnique),
}

impl Db {
    pub async fn run(&self) -> anyhow::Result<()> {
        match self {
            Db::DbPath(cmd) => cmd.run().await,
            Db::DbOpen(cmd) => cmd.run().await,
            Db::DbMinUnique(cmd) => cmd.run().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(bytes: &[u8]) -> Hash32 {
        let mut hash = Hash32::default();
        hash[..bytes.len()].copy_from_slice(bytes);
        hash
    }

    fn min_unique_hex_chars(mut hashes: Vec<Hash32>) -> usize {
        hashes.sort_unstable();
        let mut min_unique = MinUniqueHexChars::default();
        for hash in hashes {
            min_unique.add_sorted(hash);
        }
        min_unique.finish()
    }

    #[test]
    fn min_unique_hex_chars_empty() {
        assert_eq!(0, min_unique_hex_chars(vec![]));
    }

    #[test]
    fn min_unique_hex_chars_single() {
        assert_eq!(1, min_unique_hex_chars(vec![hash(&[0])]));
    }

    #[test]
    fn min_unique_hex_chars_first_nibble_differs() {
        assert_eq!(1, min_unique_hex_chars(vec![hash(&[0x00]), hash(&[0x10])]));
    }

    #[test]
    fn min_unique_hex_chars_second_nibble_differs() {
        assert_eq!(2, min_unique_hex_chars(vec![hash(&[0x00]), hash(&[0x01])]));
    }

    #[test]
    fn min_unique_hex_chars_uses_nearest_neighbors() {
        assert_eq!(
            4,
            min_unique_hex_chars(vec![
                hash(&[0xab, 0xc0]),
                hash(&[0xab, 0xc1]),
                hash(&[0xff]),
            ])
        );
    }

    #[test]
    fn min_unique_hex_chars_deduplicates_exact_hashes() {
        assert_eq!(1, min_unique_hex_chars(vec![hash(&[0x00]), hash(&[0x00])]));
    }
}
