#![allow(clippy::len_zero)]

use crate::model::{FileInfo, IMPath};
use anyhow::Result;
use imagehash::mpmc;
use std::fs::Metadata;
use std::path::{Path, PathBuf};

const SERIAL_CHANNEL_BATCH: usize = 100;

pub fn serial_scan(paths: Vec<PathBuf>) -> Result<mpmc::Receiver<(IMPath, Result<FileInfo>)>> {
    let (tx, rx) = mpmc::unbounded();

    rayon::spawn(move || {
        let mut cb = Vec::with_capacity(SERIAL_CHANNEL_BATCH);

        for path in paths {
            for entry in walkdir::WalkDir::new(path) {
                let e = match entry {
                    Ok(e) => e,
                    // TODO: propagate error? At least propagate that we failed to traverse a directory.
                    // It would be unfortunate to delete records because a scan temporarily failed.
                    Err(e) => {
                        eprintln!("error from walkdir entry: {}", e);
                        continue;
                    }
                };
                if !e.file_type().is_file() {
                    continue;
                }
                let Some(path) = e.path().to_str().map(String::from) else {
                    // Skip non-unicode paths.
                    continue;
                };

                // The serial scan primarily exists because WSL1 fast-paths the
                // readdir, serial stat access pattern. It's significantly faster
                // than a parallel crawl and random access stat pattern.
                let metadata = e.metadata().map_err(anyhow::Error::from);
                cb.push((path, metadata.map(From::from)));
                if cb.len() == cb.capacity() {
                    cb = tx.send_many(cb).unwrap();
                }
            }
        }

        if cb.len() > 0 {
            tx.send_many(cb).unwrap();
        }
    });

    Ok(rx)
}

const PARALLEL_CHANNEL_BATCH: usize = 100;
const CONCURRENCY: usize = 4;

pub fn parallel_scan(paths: Vec<PathBuf>) -> Result<mpmc::Receiver<(IMPath, Result<FileInfo>)>> {
    let (path_tx, path_rx) = mpmc::unbounded();
    let (meta_tx, meta_rx) = mpmc::unbounded();

    for path in paths {
        let tx = path_tx.clone();
        rayon::spawn(move || {
            let mut cb = Vec::with_capacity(PARALLEL_CHANNEL_BATCH);
            for entry in jwalk::WalkDir::new(&path)
                .skip_hidden(false)
                .sort(false)
                .parallelism(jwalk::Parallelism::RayonDefaultPool {
                    busy_timeout: std::time::Duration::from_secs(300),
                })
            {
                let e = match entry {
                    Ok(e) => e,
                    // TODO: propagate error? At least propagate that we failed to traverse a directory.
                    // It would be unfortunate to delete records because a scan temporarily failed.
                    Err(e) => {
                        eprintln!("error from jwalk entry: {}", e);
                        continue;
                    }
                };
                if !e.file_type().is_file() {
                    continue;
                }
                let Some(path) = e.path().to_str().map(String::from) else {
                    // Skip non-unicode paths.
                    continue;
                };

                cb.push(path);
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
                            let metadata = std::fs::symlink_metadata(&p)
                                .map(From::from)
                                .map_err(anyhow::Error::from);
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

    Ok(meta_rx)
}

#[cfg(windows)]
mod win;

type ScanFn = fn(Vec<PathBuf>) -> Result<mpmc::Receiver<(IMPath, Result<FileInfo>)>>;

#[cfg(windows)]
pub fn get_all_scanners() -> &'static [(&'static str, ScanFn)] {
    &[
        ("walkdir", serial_scan),
        ("jwalk", parallel_scan),
        ("winscan", win::windows_scan),
    ]
}

#[cfg(not(windows))]
pub fn get_all_scanners() -> &'static [(&'static str, ScanFn)] {
    &[("walkdir", serial_scan), ("jwalk", parallel_scan)]
}

#[cfg(windows)]
pub fn get_default_scan() -> ScanFn {
    // On both NTFS and over SMB, walkdir is 4-10x faster than jwalk.
    // But NtQueryDirectoryFile is faster still.
    win::windows_scan
}

#[cfg(target_os = "linux")]
pub fn get_default_scan() -> ScanFn {
    // jwalk is very slow on WSL1
    let prefer_serial = match std::fs::read_to_string("/proc/version_signature") {
        Ok(contents) => contents.contains("Microsoft"),
        Err(_) => false,
    };
    if prefer_serial {
        serial_scan
    } else {
        parallel_scan
    }
}

#[cfg(all(not(windows), not(target_os = "linux")))]
pub fn get_default_scan() -> ScanFn {
    // Default to serial on platforms where we haven't measured jwalk
    // as being faster.  It's possible jwalk would be faster on every
    // unix, especially macOS, but we should check.
    serial_scan
}
