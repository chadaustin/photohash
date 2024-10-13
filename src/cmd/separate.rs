use crate::cmd::diff::compute_difference;
use anyhow::bail;
use anyhow::Result;
use clap::Args;
use clap::ValueEnum;
use photohash::Database;
use std::fmt::Display;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::vec::Vec;

#[derive(Args)]
#[command(name = "separate", about = "Separate files missing in destination")]
pub struct Separate {
    src: PathBuf,

    #[arg(required = true)]
    dests: Vec<PathBuf>,

    /// Disregard perceptual hashes and only list files whose exact contents aren't in destination
    #[arg(long)]
    exact: bool,

    /// Create hard links to files missing in destination
    #[arg(long)]
    link: Option<PathBuf>,

    /// Move files missing in destination
    #[arg(long)]
    r#move: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
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
    pub async fn run(mut self) -> Result<()> {
        // We need to canonicalize to correctly strip_prefix later.
        self.src = self.src.canonicalize()?;
        self.dests = self
            .dests
            .iter()
            .map(|p| p.canonicalize())
            .collect::<std::io::Result<Vec<PathBuf>>>()?;

        if self.link.is_none() && self.r#move.is_none() {
            bail!("Either --link or --move required");
        }

        // To avoid accidentally changing the source directory
        // structure, disallow placing out in src.
        if let Some(out) = &self.link {
            std::fs::create_dir_all(out)?;
            if out.canonicalize()?.starts_with(&self.src) {
                bail!("May not place output within source");
            }
        }
        if let Some(out) = &self.r#move {
            std::fs::create_dir_all(out)?;
            if out.canonicalize()?.starts_with(&self.src) {
                bail!("May not place output within source");
            }
        }

        let db = Arc::new(Mutex::new(Database::open()?));

        let differences =
            compute_difference(&db, self.src.clone(), self.dests.clone(), self.exact).await?;
        if differences.missing.is_empty() {
            eprintln!("Nothing missing in destination");
            return Ok(());
        }

        for path in differences.missing {
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

            let prepare_output_path = |out: &Path| -> anyhow::Result<PathBuf> {
                let out_path = out.join(rel);
                if let Some(parent) = out_path.parent() {
                    () = std::fs::create_dir_all(parent)?;
                }
                if out_path.exists() {
                    bail!("{} already exists", out_path.display());
                }
                Ok(out_path)
            };

            if let Some(link_out) = &self.link {
                let out_path = prepare_output_path(link_out)?;
                let out_simplified = dunce::simplified(link_out);
                println!("linking {} to {}", rel.display(), out_simplified.display());
                std::fs::hard_link(&path, out_path)?;
            }

            if let Some(move_out) = &self.r#move {
                let out_path = prepare_output_path(move_out)?;
                let out_simplified = dunce::simplified(move_out);
                println!("moving {} to {}", rel.display(), out_simplified.display());
                std::fs::rename(&path, out_path)?;
            }
        }

        Ok(())
    }
}
