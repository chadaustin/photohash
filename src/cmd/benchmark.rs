use crate::cmd::index;
use anyhow::anyhow;
use chrono::DateTime;
use chrono::Local;
use clap::Args;
use clap::Subcommand;
use futures::future::join_all;
use photohash::database::Database;
use photohash::hash::get_hasher;
use photohash::hash::ContentHashSet;
use photohash::hash::ContentHashType;
use photohash::model::FileInfo;
use photohash::model::IMPath;
use photohash::scan;
use rand::RngCore;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

#[derive(Subcommand)]
#[command(name = "benchmark", about = "Benchmark directory scans")]
pub enum Benchmark {
    Walkdir(Walkdir),
    Jwalk(Jwalk),
    JwalkParStat(JwalkParStat),
    Scan(Scan),
    Index(Index),
    Validate(Validate),
    Hashes(Hashes),
}

impl Benchmark {
    pub async fn run(&self) -> anyhow::Result<()> {
        match self {
            Benchmark::Walkdir(cmd) => cmd.run().await,
            Benchmark::Jwalk(cmd) => cmd.run().await,
            Benchmark::JwalkParStat(cmd) => cmd.run().await,
            Benchmark::Scan(cmd) => cmd.run().await,
            Benchmark::Index(cmd) => cmd.run().await,
            Benchmark::Validate(cmd) => cmd.run().await,
            Benchmark::Hashes(cmd) => cmd.run().await,
        }
    }
}

#[derive(Debug, Args)]
pub struct Walkdir {
    #[arg(long)]
    stat: bool,
    path: PathBuf,
}

impl Walkdir {
    async fn run(&self) -> anyhow::Result<()> {
        let now = Instant::now();
        let mut stat_calls = 0;

        for entry in walkdir::WalkDir::new(&self.path) {
            let e = match entry {
                Ok(e) => e,
                Err(e) => {
                    eprintln!("error from walkdir: {}", e);
                    continue;
                }
            };
            if !e.file_type().is_file() {
                continue;
            }

            if self.stat {
                _ = e.metadata()?;
                stat_calls += 1;
            }
        }
        println!("walkdir: {} ms", now.elapsed().as_millis());
        println!("stat calls: {}", stat_calls);
        Ok(())
    }
}

#[derive(Debug, Args)]
pub struct Jwalk {
    #[arg(long)]
    stat: bool,
    #[arg(long)]
    bypath: bool,
    path: PathBuf,
    #[arg(long)]
    sort: bool,
}

impl Jwalk {
    async fn run(&self) -> anyhow::Result<()> {
        let now = Instant::now();
        let mut stat_calls = 0;
        for entry in jwalk::WalkDir::new(&self.path)
            .skip_hidden(false)
            .sort(self.sort)
        {
            let e = match entry {
                Ok(e) => e,
                Err(e) => {
                    eprintln!("error from jwalk: {}", e);
                    continue;
                }
            };
            if !e.file_type().is_file() {
                continue;
            }

            if self.stat {
                _ = if self.bypath {
                    std::fs::symlink_metadata(e.path())?
                } else {
                    e.metadata()?
                };
                stat_calls += 1;
            }
        }
        println!("jwalk: {} ms", now.elapsed().as_millis());
        println!("stat calls: {}", stat_calls);
        Ok(())
    }
}

#[derive(Debug, Args)]
pub struct JwalkParStat {
    path: PathBuf,
    #[arg(long)]
    sort: bool,
    #[arg(long, default_value_t = 4)]
    threads: usize,
    #[arg(long, default_value_t = 100)]
    path_batch: usize,
    #[arg(long, default_value_t = 100)]
    batch: usize,
}

impl JwalkParStat {
    async fn run(&self) -> anyhow::Result<()> {
        let now = Instant::now();

        let (path_tx, path_rx) = batch_channel::unbounded_sync();
        let (meta_tx, meta_rx) = batch_channel::unbounded();

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
                    cb = path_tx.send_vec(cb).unwrap();
                }
            }
            if !cb.is_empty() {
                path_tx.send_vec(cb).unwrap();
            }
        });

        for _ in 0..self.threads {
            let rx = path_rx.clone().into_async();
            let tx = meta_tx.clone().into_sync();
            let batch = self.batch;
            tokio::spawn(async move {
                loop {
                    let paths = rx.recv_batch(batch).await;
                    if paths.is_empty() {
                        return;
                    }
                    tx.send_iter(paths.into_iter().map(std::fs::symlink_metadata))
                        .unwrap();
                }
            });
        }
        drop(path_rx);
        drop(meta_tx);

        let mut stat_calls = 0;
        loop {
            let metas = meta_rx.recv_batch(self.batch).await;
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

#[derive(Debug, Args)]
pub struct Scan {
    path: PathBuf,
    #[arg(long, default_value_t = 1)]
    batch: usize,
    #[arg(long)]
    scanner: Option<String>,
}

impl Scan {
    async fn run(&self) -> anyhow::Result<()> {
        let scan = self
            .scanner
            .as_ref()
            .map(|name| {
                for (n, s) in scan::get_all_scanners() {
                    if *n == name {
                        return Ok(*s);
                    }
                }
                Err(anyhow!("unknown scanner {}", name))
            })
            .unwrap_or(Ok(scan::get_default_scan()))?;

        let now = Instant::now();

        let mut metadata_results = 0;

        let rx = scan(&[&self.path])?;
        loop {
            let results = rx.recv_batch(self.batch).await;
            if results.is_empty() {
                break;
            }
            for (_path, metadata_result) in results {
                let _ = metadata_result?;
                metadata_results += 1;
            }
        }

        println!("scan: {} ms", now.elapsed().as_millis());
        println!("metadata results: {}", metadata_results);
        Ok(())
    }
}

#[derive(Debug, Args)]
pub struct Index {
    path: PathBuf,

    #[arg(long)]
    real_db: bool,
}

impl Index {
    async fn run(&self) -> anyhow::Result<()> {
        let db = Arc::new(Mutex::new(if self.real_db {
            Database::open()?
        } else {
            Database::open_memory()?
        }));

        let now = Instant::now();

        let mut pfr_results = 0;

        let mut rx = index::do_index(&db, &[&self.path], false)?;
        while let Some(jh) = rx.recv().await {
            let _: index::ProcessFileResult = jh.await.unwrap()?;
            pfr_results += 1;
        }

        println!("index: {} ms", now.elapsed().as_millis());
        println!("file results: {}", pfr_results);
        Ok(())
    }
}

#[derive(Debug, Args)]
pub struct Validate {
    path: PathBuf,
}

impl Validate {
    async fn run(&self) -> anyhow::Result<()> {
        let scanners = scan::get_all_scanners();
        let mut results = Vec::with_capacity(scanners.len());
        let mut all_scanners = HashSet::new();
        for (scanner_name, scan) in scanners {
            all_scanners.insert(scanner_name);
            let path = self.path.clone();
            results.push(tokio::spawn(async move {
                let mut metadata = HashMap::new();
                let rx = scan(&[&path])?;
                while let Some((path, meta)) = rx.recv().await {
                    //eprintln!("{}: {}", scanner_name, path);
                    metadata.insert(path, meta);
                }
                Ok::<_, anyhow::Error>((*scanner_name, metadata))
            }));
        }

        let results: Vec<_> = join_all(results)
            .await
            .into_iter()
            .map(|f| f.unwrap())
            .collect::<anyhow::Result<_>>()?;

        // path -> {scanner_name -> metadata}
        let mut path_to_scanner: HashMap<IMPath, HashMap<&'static str, FileInfo>> = HashMap::new();

        for (scanner_name, path_to_metadata) in results {
            for (path, metadata) in path_to_metadata {
                path_to_scanner
                    .entry(path)
                    .or_default()
                    .insert(scanner_name, metadata?);
            }
        }

        for (path, scan_results) in path_to_scanner {
            for missing in &all_scanners - &scan_results.keys().collect() {
                eprintln!("{} missing in {}", path, missing);
            }

            let metadatas = scan_results.iter().collect::<Vec<_>>();
            let a = metadatas[0];
            for b in &metadatas[1..] {
                Self::compare(&path, a, *b);
            }
        }

        Ok(())
    }

    fn compare(path: &IMPath, a: (&&'static str, &FileInfo), b: (&&'static str, &FileInfo)) {
        let (a_scanner, a) = a;
        let (b_scanner, b) = b;
        if a.inode != b.inode {
            eprintln!(
                "{}: inodes differ: {}={}, {}={}",
                path, a_scanner, a.inode, b_scanner, b.inode
            );
        }
        if a.size != b.size {
            eprintln!(
                "{}: sizes differ: {}={}, {}={}",
                path, a_scanner, a.size, b_scanner, b.size
            );
        }
        if a.mtime != b.mtime {
            eprintln!(
                "{}: mtimes differ: {}={}, {}={}",
                path,
                a_scanner,
                DateTime::<Local>::from(a.mtime),
                b_scanner,
                DateTime::<Local>::from(b.mtime)
            );
        }
    }
}

#[derive(Debug, Args)]
pub struct Hashes {}

impl Hashes {
    async fn run(&self) -> anyhow::Result<()> {
        let mut reciprocal_throughput = 0.0;
        for c in ContentHashSet::all() {
            print!("{c:?}: ");
            std::io::stdout().flush()?;
            let throughput = self.compute_throughput(c);
            println!("{:.2} MB/s", throughput / (1000.0 * 1000.0));
            reciprocal_throughput += 1.0 / throughput;
        }
        println!("-----");
        println!(
            "total: {:.2} MB/s",
            (1.0 / reciprocal_throughput) / (1000.0 * 1000.0)
        );
        Ok(())
    }

    fn compute_throughput(&self, c: ContentHashType) -> f64 {
        let mut hasher = get_hasher(c);

        const BUF_SIZE: usize = 64 * 1024;
        let mut buf = [0u8; BUF_SIZE];
        rand::rng().fill_bytes(&mut buf);

        let start = Instant::now();
        let until = start + Duration::from_secs(5);

        // Run one iteration to ensure hashed_bytes != 0.
        hasher.update(&buf);
        let mut hashed_bytes = BUF_SIZE;

        while Instant::now() < until {
            hasher.update(&buf);
            hashed_bytes += BUF_SIZE;
        }
        hasher.finalize();
        let end = Instant::now();
        // Safety: end - start cannot be zero.
        (hashed_bytes as f64) / (end - start).as_secs_f64()
    }
}
