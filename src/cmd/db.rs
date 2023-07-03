#[cfg(not(unix))]
use anyhow::anyhow;
use anyhow::Context;
use anyhow::Result;
use imagehash::database;
#[cfg(not(windows))]
use std::os::unix::process::CommandExt;
use std::process::Command;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "path", about = "Print database location")]
pub struct DbPath {}

impl DbPath {
    async fn run(&self) -> Result<()> {
        println!("{}", database::get_database_path()?.display());
        Ok(())
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "open", about = "Interactively explore database")]
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

#[derive(Debug, StructOpt)]
#[structopt(name = "db", about = "Database administration")]
pub enum Db {
    #[structopt(name = "path")]
    DbPath(DbPath),
    #[structopt(name = "open")]
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
