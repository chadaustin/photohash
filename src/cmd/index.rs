use anyhow::{Context, Result};
use hex::ToHex;
use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::sync::mpsc;
use walkdir::WalkDir;

use crate::compute_blake3;
use crate::heic_perceptual_hash;
use crate::jpeg_perceptual_hash;
use crate::model::ImageMetadata;
use crate::ContentMetadata;
use crate::Database;
use crate::FileInfo;

const PATH_CHANNEL_SIZE: usize = 8;
const RESULT_CHANNEL_SIZE: usize = 8;

#[derive(Debug, StructOpt)]
#[structopt(name = "index", about = "Scan directories and update the index")]
pub struct Index {
    #[structopt(parse(from_os_str))]
    dirs: Vec<PathBuf>,
}

impl Index {
    pub async fn run(&self) -> Result<()> {
        let db = Arc::new(Database::open()?);

        let dirs = if self.dirs.is_empty() {
            vec![".".into()]
        } else {
            self.dirs.clone()
        };

        let dirs: Vec<PathBuf> = dirs
            .iter()
            .map(|path| {
                path.canonicalize()
                    .with_context(|| format!("failed to canonicalize {}", path.display()))
            })
            .collect::<Result<_, _>>()?;

        let (path_tx, mut path_rx) = mpsc::channel(PATH_CHANNEL_SIZE);

        // Crawler task pushes work into path_tx.
        tokio::spawn(async move {
            for dir in dirs {
                for entry in WalkDir::new(dir) {
                    if let Ok(e) = entry {
                        if !e.file_type().is_file() {
                            continue;
                        }

                        // We could defer the stat() to the pulling thread, but:
                        // 1. We just read the directory, so maybe stat() is hot
                        // 2. Windows may provide some or all of this information
                        //    from the FindNextFile call.
                        let metadata = FileInfo::from_dir_entry(&e);

                        if let Err(_) = path_tx.send((e.into_path(), metadata)).await {
                            eprintln!("receiver dropped");
                            return;
                        }
                    }
                }
            }
        });

        let (metadata_tx, mut metadata_rx) = mpsc::channel(RESULT_CHANNEL_SIZE);

        // Reads enumerated paths and computes necessary file metadata and content hashes.
        let db2 = db.clone();
        tokio::spawn(async move {
            while let Some((path, metadata)) = path_rx.recv().await {
                let metadata = match metadata {
                    Ok(metadata) => metadata,
                    Err(err) => {
                        eprintln!("failed to read metadata of {}: {}", path.display(), err);
                        continue;
                    }
                };

                // Check the database to see if there's anything to recompute.
                let db_metadata = match db.get_file(&path) {
                    Ok(record) => record,
                    Err(err) => {
                        eprintln!("failed to read record for {}, {}", path.display(), err);
                        continue;
                    }
                };

                let metadata_future = tokio::spawn({
                    let db = db.clone();
                    async move { process_file(db, path, metadata, db_metadata).await }
                });

                if let Err(_) = metadata_tx.send(metadata_future).await {
                    eprintln!("receiver dropped");
                    return;
                }
            }
        });

        let db = db2;
        while let Some(content_metadata_future) = metadata_rx.recv().await {
            let content_metadata_future = content_metadata_future.await?;
            let (content_metadata, image_metadata) = match content_metadata_future {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("failed to read the thing {}", e);
                    continue;
                }
            };
            println!(
                "{}: size = {}, blake3 = {}, blockhash = {}",
                content_metadata.path.display(),
                content_metadata.file_info.size,
                content_metadata.blake3.encode_hex::<String>(),
                image_metadata
                    .as_ref()
                    .map_or("none".into(), |im| hex::encode(&im.blockhash256)),
            );
            db.add_files(&[&content_metadata])?;
            if let Some(im) = image_metadata {
                db.add_image_metadata(&content_metadata.blake3, &im)?;
            }
        }

        /*
                let (paths_sender, paths_receiver) = unbounded();
                rayon::spawn(move || {
                    for entry in WalkDir::new(".") {
                        if let Ok(e) = entry {
                            if e.file_type().is_file() {
                                paths_sender.send(e.into_path()).unwrap();
                            }
                        }
                    }
                });
                let (outputs_sender, outputs_receiver) = unbounded();
                rayon::spawn(move || {
                    for path in paths_receiver {
                        let (output_sender, output_receiver) = channel();
                        let (pool_type, hasher) = get_hasher(&path);
                        let pool = match pool_type {
                            PoolType::Cpu => &cpu_pool,
                            PoolType::Io => &io_pool,
                        };
                        outputs_sender.send(output_receiver).unwrap();
                        pool.spawn_fifo(move || {
                            let hash = hasher(&path);
                            output_sender.send((path, hash)).unwrap_or(())
                        });
                    }
                });
                let mut stdout = std::io::stdout();
                for output in outputs_receiver {
                    let (path, hash) = output.await?;
                    match hash {
                        Ok(hash) => write!(
                            &mut stdout,
                            "{} *{}\n",
                            hash.encode_hex::<String>(),
                            path.display()
                        )?,
                        Err(e) => eprintln!("hashing {} failed: {}", path.display(), e),
                    }
                }
        */
        Ok(())
    }
}

async fn process_file(
    db: Arc<Database>,
    path: PathBuf,
    file_info: FileInfo,
    db_metadata: Option<ContentMetadata>,
) -> Result<(ContentMetadata, Option<ImageMetadata>)> {
    // TODO: only open the file once, and reuse it for any potential image hashing
    let b3 = match db_metadata {
        Some(ref record) => {
            // If metadata matches our records, we can assume blake3 hasn't changed.
            if file_info == record.file_info {
                record.blake3
            } else {
                compute_blake3(&path).await?
            }
        }
        None => {
            // No record of this file - blake3 must be computed.
            eprintln!("computing blake3 of {}", path.display());
            compute_blake3(&path).await?
        }
    };

    let mut image_metadata = None;

    // TODO: is this an image?
    if let Some(ext) = path.extension() {
        if ext.eq_ignore_ascii_case("jpg") || ext.eq_ignore_ascii_case("jpeg") {
            image_metadata = Some(match db.get_image_metadata(&b3)? {
                Some(im) => im,
                None => jpeg_perceptual_hash(&path)?,
            });
        } else if ext.eq_ignore_ascii_case("heic") {
            image_metadata = Some(match db.get_image_metadata(&b3)? {
                Some(im) => im,
                None => heic_perceptual_hash(&path)?,
            });
        }
    }

    Ok((
        ContentMetadata {
            path,
            file_info,
            blake3: b3,
        },
        image_metadata,
    ))
}