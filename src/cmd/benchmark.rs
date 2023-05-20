use crate::mpmc;
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
    #[structopt(long)]
    sort: bool,
}

impl Jwalk {
    async fn run(&self) -> Result<()> {
        let now = Instant::now();
        let mut stat_calls = 0;
        for entry in jwalk::WalkDir::new(&self.path)
            .skip_hidden(false)
            .sort(self.sort)
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
pub struct JwalkParStat {
    #[structopt(parse(from_os_str))]
    path: PathBuf,
    #[structopt(long)]
    sort: bool,
    #[structopt(long, default_value = "4")]
    threads: usize,
    #[structopt(long, default_value = "100")]
    path_batch: usize,
    #[structopt(long, default_value = "100")]
    batch: usize,
}

impl JwalkParStat {
    async fn run(&self) -> Result<()> {
        let now = Instant::now();

        let (path_tx, path_rx) = mpmc::unbounded();
        let (meta_tx, meta_rx) = mpmc::unbounded();

        let path = self.path.clone();
        let sort = self.sort;
        let path_batch = self.path_batch;
        tokio::spawn(async move {
            let mut cb = Vec::with_capacity(path_batch);
            for entry in jwalk::WalkDir::new(&path).skip_hidden(false).sort(sort) {
                let e = match entry {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                if !e.file_type().is_file() {
                    continue;
                }
                cb.push(e.path());
                if cb.len() == path_batch {
                    cb = path_tx.send_many(cb).unwrap();
                }
            }
            if cb.len() > 0 {
                path_tx.send_many(cb);
            }
        });

        for _ in 0..self.threads {
            let rx = path_rx.clone();
            let tx = meta_tx.clone();
            let batch = self.batch;
            tokio::spawn(async move {
                loop {
                    let paths = rx.recv_many(batch).await;
                    if paths.is_empty() {
                        return;
                    }
                    tx.send_many(paths.into_iter().map(std::fs::metadata).collect::<Vec<_>>());
                }
            });
        }
        drop(path_rx);
        drop(meta_tx);

        let mut stat_calls = 0;
        loop {
            let metas = meta_rx.recv_many(self.batch).await;
            if metas.is_empty() {
                break;
            }
            stat_calls += metas.len();
        }

        println!("jwalk-par-stat: {} ms", now.elapsed().as_millis());
        println!("stat calls: {}", stat_calls);
        Ok(())
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "benchmark", about = "Benchmark directory scans")]
pub enum Benchmark {
    Walkdir(Walkdir),
    Jwalk(Jwalk),
    JwalkParStat(JwalkParStat),
}

impl Benchmark {
    pub async fn run(&self) -> Result<()> {
        match self {
            Benchmark::Walkdir(cmd) => cmd.run().await,
            Benchmark::Jwalk(cmd) => cmd.run().await,
            Benchmark::JwalkParStat(cmd) => cmd.run().await,
        }
    }
}
