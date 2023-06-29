#![allow(clippy::len_zero)]

use crate::cmd::index;
use anyhow::anyhow;
use anyhow::Result;
use chrono::DateTime;
use chrono::Local;
use futures::future::join_all;
use imagehash::database::Database;
use imagehash::model::FileInfo;
use imagehash::model::IMPath;
use imagehash::mpmc;
use imagehash::scan;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "benchmark", about = "Benchmark directory scans")]
pub enum Benchmark {
    Walkdir(Walkdir),
    Jwalk(Jwalk),
    JwalkParStat(JwalkParStat),
    Scan(Scan),
    Index(Index),
    Validate(Validate),
}

impl Benchmark {
    pub async fn run(&self) -> Result<()> {
        match self {
            Benchmark::Walkdir(cmd) => cmd.run().await,
            Benchmark::Jwalk(cmd) => cmd.run().await,
            Benchmark::JwalkParStat(cmd) => cmd.run().await,
            Benchmark::Scan(cmd) => cmd.run().await,
            Benchmark::Index(cmd) => cmd.run().await,
            Benchmark::Validate(cmd) => cmd.run().await,
        }
    }
}

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
                        std::fs::symlink_metadata(e.path())?
                    } else {
                        e.metadata()?
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
                path_tx.send_many(cb).unwrap();
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
                    tx.send_many(
                        paths
                            .into_iter()
                            .map(std::fs::symlink_metadata)
                            .collect::<Vec<_>>(),
                    )
                    .unwrap();
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
pub struct Scan {
    #[structopt(parse(from_os_str))]
    path: PathBuf,
    #[structopt(long, default_value = "1")]
    batch: usize,
    #[structopt(long)]
    scanner: Option<String>,
}

impl Scan {
    async fn run(&self) -> Result<()> {
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

        let rx = scan(vec![self.path.clone()])?;
        loop {
            let results = rx.recv_many(self.batch).await;
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

#[derive(Debug, StructOpt)]
pub struct Index {
    #[structopt(parse(from_os_str))]
    path: PathBuf,

    #[structopt(long)]
    real_db: bool,
}

impl Index {
    async fn run(&self) -> Result<()> {
        let db = Arc::new(Mutex::new(if self.real_db {
            Database::open()?
        } else {
            Database::open_memory()?
        }));

        let now = Instant::now();

        let mut pfr_results = 0;

        let mut rx = index::do_index(&db, vec![self.path.clone()])?;
        while let Some(jh) = rx.recv().await {
            let _: index::ProcessFileResult = jh.await.unwrap()?;
            pfr_results += 1;
        }

        println!("index: {} ms", now.elapsed().as_millis());
        println!("file results: {}", pfr_results);
        Ok(())
    }
}

#[derive(Debug, StructOpt)]
pub struct Validate {
    #[structopt(parse(from_os_str))]
    path: PathBuf,
}

impl Validate {
    async fn run(&self) -> Result<()> {
        let scanners = scan::get_all_scanners();
        let mut results = Vec::with_capacity(scanners.len());
        let mut all_scanners = HashSet::new();
        for (scanner_name, scan) in scanners {
            all_scanners.insert(scanner_name);
            let path = self.path.clone();
            results.push(tokio::spawn(async move {
                let mut metadata = HashMap::new();
                let rx = scan(vec![path])?;
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
            .collect::<Result<_>>()?;

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
