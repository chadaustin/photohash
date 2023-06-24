use crate::cmd::diff::compute_difference;
use crate::cmd::index;
use crate::Database;
use anyhow::bail;
use anyhow::Result;
use std::collections::HashMap;
use std::fmt::Display;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::vec::Vec;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "separate", about = "Separate files missing in destination")]
pub struct Separate {
    #[structopt(long, parse(from_os_str))]
    out: PathBuf,
    #[structopt(parse(from_os_str))]
    src: PathBuf,
    #[structopt(parse(from_os_str), required(true))]
    dests: Vec<PathBuf>,

    #[structopt(long)]
    mode: Mode,
}

#[derive(Clone, Copy, Debug, StructOpt)]
pub enum Mode {
    Link,
    Move,
}

#[derive(Debug)]
pub struct BadMode;

impl Display for BadMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("mode must be 'link' or 'move'")
    }
}

impl FromStr for Mode {
    type Err = BadMode;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "link" => Ok(Self::Link),
            "move" => Ok(Self::Move),
            _ => Err(BadMode),
        }
    }
}

impl Separate {
    pub async fn run(&self) -> Result<()> {
        // We need to canonicalize to correctly strip_prefix later.
        let dests: Vec<PathBuf> = self
            .dests
            .iter()
            .map(|p| p.canonicalize())
            .collect::<std::io::Result<Vec<PathBuf>>>()?;

        Separate {
            out: self.out.clone(),
            src: self.src.canonicalize()?,
            dests,
            mode: self.mode,
        }
        .do_run()
        .await
    }

    async fn do_run(&self) -> Result<()> {
        let db = Arc::new(Mutex::new(Database::open()?));
        let out_simplified = dunce::simplified(&self.out);

        // To avoid accidentally changing the source directory
        // structure, disallow placing out in src.
        if self.out.starts_with(&self.src) {
            bail!("May not place output within source");
        }

        let difference = compute_difference(&db, self.src.clone(), self.dests.clone()).await?;
        if difference.is_empty() {
            eprintln!("Nothing missing in destination");
            return Ok(());
        }

        for path in difference {
            let path = PathBuf::from(path);
            let rel = match path.strip_prefix(&self.src) {
                Ok(rel) => rel,
                Err(_) => {
                    bail!(
                        "missing {} not relative to source {}",
                        path.display(),
                        self.src.display()
                    );
                }
            };
            let dest_path = self.out.join(rel);

            if let Some(parent) = dest_path.parent() {
                () = std::fs::create_dir_all(parent)?;
            }
            if dest_path.exists() {
                anyhow::bail!("{} already exists", dest_path.display());
            }
            let verb = match self.mode {
                Mode::Link => "linking",
                Mode::Move => "moving",
            };
            eprintln!("{verb} {} to {}", rel.display(), out_simplified.display());
            match self.mode {
                Mode::Link => {
                    () = std::fs::hard_link(path, dest_path)?;
                }
                Mode::Move => {
                    () = std::fs::rename(path, dest_path)?;
                }
            }
        }

        Ok(())
    }
}
