use crate::model::FileInfo;
use crate::model::IMPath;
use anyhow::Context;
use futures::FutureExt;
use std::path::Path;
use std::path::PathBuf;

const BATCH_SIZE: usize = 100;
const PATH_CHANNEL_CAPACITY: usize = 100000;
const META_CHANNEL_CAPACITY: usize = 100000;
const IO_CONCURRENCY: usize = 4;

fn canonicalize_all(paths: &[&Path]) -> anyhow::Result<Vec<PathBuf>> {
    paths
        .iter()
        .map(|path| {
            path.canonicalize()
                .with_context(|| format!("failed to canonicalize {}", path.display()))
        })
        .collect()
}

/*
 * Within which pools should directory traversals run? When cold, it's
 * an IO-bound workload. When readdir() and stat() are in cache, it's
 * a CPU-bound workload.
 *
 * Running in a dedicated thread risks competing with other num-cpu
 * pools like tokio or rayon. Running in tokio risks stalling other
 * work with blocking IO.
 *
 * The unix IO model gives no right answer. Windows is better:
 * asynchronous NtQueryDirectoryFile allows largely decoupling CPU
 * concurrency from IO concurrency.
 */

fn walkdir_scan(
    paths: &[&Path],
) -> anyhow::Result<batch_channel::Receiver<(IMPath, anyhow::Result<FileInfo>)>> {
    let paths = canonicalize_all(paths)?;

    // Bounding this pool would allow relinquishing this thread when
    // crawl has gotten too far ahead.
    let (tx, rx) = batch_channel::bounded(META_CHANNEL_CAPACITY);

    tokio::spawn(tx.autobatch_or_cancel(BATCH_SIZE, |tx| {
        async move {
            // TODO: walk each path in parallel.
            for path in paths {
                for entry in walkdir::WalkDir::new(path)
                    .follow_links(false)
                    .same_file_system(true)
                {
                    let e = match entry {
                        Ok(e) => e,
                        Err(e) => {
                            // If we have a path, propagate the error.
                            if let Some(path) = e.path().and_then(|p| p.to_str()) {
                                tx.send((path.to_string(), Err(e.into()))).await?;
                            } else {
                                // No path or non-unicode path. Be conservative for now and panic.
                                // It would be unfortunate to delete records because a scan temporarily failed.
                                panic!("Unexpected error from walkdir: {}", e);
                            }
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
                    tx.send((path, metadata.map(From::from))).await?;
                }
            }
            Ok(())
        }
        .boxed()
    }));

    Ok(rx)
}

/*
 * jwalk uses rayon internally so it seems appropriate to use rayon as
 * well. Unfortunately, while jwalk issues readdir in parallel, the
 * stat() calls happen serially after the fact. This makes jwalk quite
 * a lot slower than walkdir on WSL1 and Windows, and not much faster
 * on Linux or macOS.
 */

fn jwalk_scan(
    paths: &[&Path],
) -> anyhow::Result<batch_channel::Receiver<(IMPath, anyhow::Result<FileInfo>)>> {
    let paths = canonicalize_all(paths)?;

    let (path_tx, path_rx) = batch_channel::bounded(PATH_CHANNEL_CAPACITY);
    let (meta_tx, meta_rx) = batch_channel::bounded(META_CHANNEL_CAPACITY);

    for path in paths {
        let tx = path_tx.clone();
        tokio::spawn(tx.autobatch_or_cancel(BATCH_SIZE, |tx| {
            async move {
                // No same_file_system option. https://github.com/Byron/jwalk/issues/27
                for entry in jwalk::WalkDir::new(&path)
                    .skip_hidden(false)
                    .sort(false)
                    .follow_links(false)
                    .parallelism(jwalk::Parallelism::RayonDefaultPool {
                        busy_timeout: std::time::Duration::from_secs(300),
                    })
                {
                    let e = match entry {
                        Ok(e) => e,
                        Err(e) => {
                            // If we have a path, propagate the error.
                            if let Some(path) = e.path().and_then(|p| p.to_str()) {
                                tx.send((path.to_string(), Err(e.into()))).await?;
                            } else {
                                // No path or non-unicode path. Be conservative for now and panic.
                                // It would be unfortunate to delete records because a scan temporarily failed.
                                panic!("Unexpected error from jwalk: {}", e);
                            }
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

                    tx.send((path, Ok(()))).await?;
                }
                Ok(())
            }
            .boxed()
        }));
    }
    drop(path_tx);

    for _ in 0..IO_CONCURRENCY {
        let rx = path_rx.clone();
        let tx = meta_tx.clone();
        tokio::spawn(async move {
            loop {
                let paths = rx.recv_batch(BATCH_SIZE).await;
                if paths.is_empty() {
                    return;
                }
                let Ok(()) = tx
                    .send_iter(paths.into_iter().map(|(p, result)| match result {
                        Ok(()) => {
                            let metadata = std::fs::symlink_metadata(&p)
                                .map(From::from)
                                .map_err(anyhow::Error::from);
                            (p, metadata)
                        }
                        Err(e) => (p, Err(e)),
                    }))
                    .await
                else {
                    // Receiver dropped; we can early-exit.
                    return;
                };
            }
        });
    }
    drop(path_rx);
    drop(meta_tx);

    Ok(meta_rx)
}

#[cfg(windows)]
mod win;

// Optimization opportunity: &[&Path] could avoid allocation at some
// callsites as &mut dyn Iterator<Item = &Path>
type ScanFn =
    fn(&[&Path]) -> anyhow::Result<batch_channel::Receiver<(IMPath, anyhow::Result<FileInfo>)>>;

#[cfg(windows)]
pub fn get_all_scanners() -> &'static [(&'static str, ScanFn)] {
    &[
        ("walkdir", walkdir_scan),
        ("jwalk", jwalk_scan),
        ("winscan", win::windows_scan),
    ]
}

#[cfg(not(windows))]
pub fn get_all_scanners() -> &'static [(&'static str, ScanFn)] {
    &[("walkdir", walkdir_scan), ("jwalk", jwalk_scan)]
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
        walkdir_scan
    } else {
        jwalk_scan
    }
}

#[cfg(all(not(windows), not(target_os = "linux")))]
pub fn get_default_scan() -> ScanFn {
    // Default to serial on platforms where we haven't measured jwalk
    // as being faster.  It's possible jwalk would be faster on every
    // unix, especially macOS, but we should check.
    walkdir_scan
}
