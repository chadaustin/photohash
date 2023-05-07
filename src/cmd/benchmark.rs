use anyhow::Result;
use std::path::PathBuf;
use std::time::Instant;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct Walkdir {
    #[structopt(long)]
    stat: bool,
    #[structopt(parse(from_os_str))]
    path: PathBuf,
}

impl Walkdir {
    async fn run(&self) -> Result<()> {
        let now = Instant::now();
        let mut stat_calls = 0;
        for entry in walkdir::WalkDir::new(&self.path) {
            if let Ok(e) = entry {
                if !e.file_type().is_file() {
                    continue;
                }

                if self.stat {
                    _ = e.metadata()?;
                    stat_calls += 1;
                }
            }
        }
        println!("walkdir: {} ms", now.elapsed().as_millis());
        println!("stat calls: {}", stat_calls);
        Ok(())
    }
}

#[derive(Debug, StructOpt)]
pub struct Jwalk {
    #[structopt(long)]
    stat: bool,
    #[structopt(long)]
    bypath: bool,
    #[structopt(parse(from_os_str))]
    path: PathBuf,
}

impl Jwalk {
    async fn run(&self) -> Result<()> {
        let now = Instant::now();
        let mut stat_calls = 0;
        for entry in jwalk::WalkDir::new(&self.path)
            .skip_hidden(false)
            .sort(true)
        {
            if let Ok(e) = entry {
                if !e.file_type().is_file() {
                    continue;
                }

                if self.stat {
                    _ = if self.bypath {
                        std::fs::metadata(e.path())?;
                    } else {
                        e.metadata()?;
                    };
                    stat_calls += 1;
                }
            }
        }
        println!("jwalk: {} ms", now.elapsed().as_millis());
        println!("stat calls: {}", stat_calls);
        Ok(())
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "benchmark", about = "Benchmark directory scans")]
pub enum Benchmark {
    Walkdir(Walkdir),
    Jwalk(Jwalk),
}

impl Benchmark {
    pub async fn run(&self) -> Result<()> {
        match self {
            Benchmark::Walkdir(cmd) => cmd.run().await,
            Benchmark::Jwalk(cmd) => cmd.run().await,
        }
    }
}
