use crate::mpmc;
use anyhow::Result;
use std::fs::Metadata;
use std::path::{Path, PathBuf};

const SERIAL_CHANNEL_BATCH: usize = 100;

pub fn serial_scan(paths: Vec<PathBuf>) -> mpmc::Receiver<(PathBuf, Result<Metadata>)> {
    let (tx, rx) = mpmc::unbounded();

    rayon::spawn(move || {
        let mut cb = Vec::with_capacity(SERIAL_CHANNEL_BATCH);

        for path in paths {
            for entry in walkdir::WalkDir::new(path) {
                let e = match entry {
                    Ok(e) => e,
                    // TODO: propagate error?
                    Err(_) => continue,
                };
                if !e.file_type().is_file() {
                    continue;
                }

                // The serial scan primarily exists because WSL1 fast-paths the
                // readdir, serial stat access pattern. It's significantly faster
                // than a parallel crawl and random access stat pattern.
                let metadata = e.metadata().map_err(anyhow::Error::from);
                cb.push((e.into_path(), metadata));
                if cb.len() == cb.capacity() {
                    cb = tx.send_many(cb).unwrap();
                }
            }
        }

        if cb.len() > 0 {
            tx.send_many(cb).unwrap();
        }
    });

    rx
}

const PARALLEL_CHANNEL_BATCH: usize = 100;
const CONCURRENCY: usize = 4;

pub fn parallel_scan(paths: Vec<PathBuf>) -> mpmc::Receiver<(PathBuf, Result<Metadata>)> {
    let (path_tx, path_rx) = mpmc::unbounded();
    let (meta_tx, meta_rx) = mpmc::unbounded();

    for path in paths {
        let tx = path_tx.clone();
        rayon::spawn(move || {
            let mut cb = Vec::with_capacity(PARALLEL_CHANNEL_BATCH);
            for entry in jwalk::WalkDir::new(&path).skip_hidden(false).sort(false) {
                let e = match entry {
                    Ok(e) => e,
                    // TODO: propagate error?
                    Err(_) => continue,
                };
                if !e.file_type().is_file() {
                    continue;
                }

                cb.push(e.path());
                if cb.len() == cb.capacity() {
                    cb = tx.send_many(cb).unwrap();
                }
            }
            if cb.len() > 0 {
                tx.send_many(cb).unwrap();
            }
        });
    }
    drop(path_tx);

    for _ in 0..CONCURRENCY {
        let rx = path_rx.clone();
        let tx = meta_tx.clone();
        tokio::spawn(async move {
            loop {
                let paths = rx.recv_many(PARALLEL_CHANNEL_BATCH).await;
                if paths.is_empty() {
                    return;
                }
                tx.send_many(
                    paths
                        .into_iter()
                        .map(|p| {
                            let metadata =
                                std::fs::symlink_metadata(&p).map_err(anyhow::Error::from);
                            (p, metadata)
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap();
            }
        });
    }
    drop(path_rx);
    drop(meta_tx);

    meta_rx
}

#[cfg(target_os = "linux")]
fn is_wsl1() -> bool {
    let check = || -> std::io::Result<bool> {
        let contents = std::fs::read_to_string("/proc/version_signature")?;
        Ok(contents.contains("Microsoft"))
    };
    check().unwrap_or(false)
}

#[cfg(not(target_os = "linux"))]
fn is_wsl1() -> bool {
    false
}

pub fn get_scan() -> fn(Vec<PathBuf>) -> mpmc::Receiver<(PathBuf, Result<Metadata>)> {
    if is_wsl1() {
        serial_scan
    } else {
        parallel_scan
    }
}
