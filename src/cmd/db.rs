#[cfg(not(unix))]
use anyhow::anyhow;
use anyhow::Context;
use anyhow::Result;
use clap::Args;
use clap::Subcommand;
use photohash::database;
#[cfg(not(windows))]
use std::os::unix::process::CommandExt;
use std::process::Command;

#[derive(Debug, Args)]
#[command(name = "path", about = "Print database location")]
pub struct DbPath {}

impl DbPath {
    async fn run(&self) -> Result<()> {
        println!("{}", database::get_database_path()?.display());
        Ok(())
    }
}

#[derive(Debug, Args)]
#[command(name = "open", about = "Interactively explore database")]
pub struct DbOpen {}

impl DbOpen {
    async fn run(&self) -> Result<()> {
        let mut cmd = Command::new("sqlite3");
        cmd.arg(database::get_database_path()?);
        Self::exec(cmd).context("failed to run sqlite3")
    }

    #[cfg(unix)]
    fn exec(mut cmd: Command) -> Result<()> {
        Err(cmd.exec().into())
    }

    #[cfg(not(unix))]
    fn exec(mut cmd: Command) -> Result<()> {
        let status = cmd.status()?;
        if status.success() {
            Ok(())
        } else {
            Err(anyhow!("Failed to run sqlite3 command"))
        }
    }
}

#[derive(Debug, Subcommand)]
#[command(name = "db", about = "Database administration")]
pub enum Db {
    #[command(name = "path")]
    DbPath(DbPath),
    #[command(name = "open")]
    DbOpen(DbOpen),
}

impl Db {
    pub async fn run(&self) -> Result<()> {
        match self {
            Db::DbPath(cmd) => cmd.run().await,
            Db::DbOpen(cmd) => cmd.run().await,
        }
    }
}
