use crate::compute_blake3;
use anyhow::Context;
use anyhow::Result;
use hex::ToHex;
use imagehash::model::ContentMetadata;
use imagehash::model::FileInfo;
use imagehash::model::IMPath;
use imagehash::model::ImageMetadata;
use imagehash::scan;
use imagehash::Database;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use structopt::StructOpt;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

const RESULT_CHANNEL_SIZE: usize = 8;

#[derive(Debug, StructOpt)]
#[structopt(name = "index", about = "Scan directories and update the index")]
pub struct Index {
    #[structopt(parse(from_os_str), required(true))]
    dirs: Vec<PathBuf>,
}

impl Index {
    pub async fn run(&self) -> Result<()> {
        let db = Arc::new(Mutex::new(Database::open()?));

        let dirs = if self.dirs.is_empty() {
            vec![".".into()]
        } else {
            self.dirs.clone()
        };

        let mut metadata_rx = do_index(&db, dirs)?;

        while let Some(content_metadata_future) = metadata_rx.recv().await {
            let content_metadata_future = content_metadata_future.await?;
            let process_file_result = match content_metadata_future {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("failed to read the thing {}", e);
                    continue;
                }
            };
            let content_metadata = process_file_result.content_metadata;
            let image_metadata = process_file_result.image_metadata;
            if process_file_result.blake3_computed {
                println!(
                    "{} {:>8}K {}",
                    content_metadata.blake3.encode_hex::<String>(),
                    content_metadata.file_info.size / 1024,
                    dunce::simplified(process_file_result.path.as_ref()).display(),
                );
            }
            if let Some(im) = image_metadata {
                db.lock()
                    .unwrap()
                    .add_image_metadata(&content_metadata.blake3, &im)?;
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

pub fn do_index(
    db: &Arc<Mutex<Database>>,
    dirs: Vec<PathBuf>,
) -> Result<mpsc::Receiver<JoinHandle<Result<ProcessFileResult>>>> {
    // We have to canonicalize the paths.
    let dirs: Vec<PathBuf> = dirs
        .into_iter()
        .map(|path| {
            path.canonicalize()
                .with_context(|| format!("failed to canonicalize {}", path.display()))
        })
        .collect::<Result<_, _>>()?;

    let scanner = scan::get_default_scan();
    let path_meta_rx = scanner(dirs)?;

    let (metadata_tx, metadata_rx) = mpsc::channel(RESULT_CHANNEL_SIZE);

    // Reads enumerated paths and computes necessary file metadata and content hashes.
    let db = db.clone();
    tokio::spawn(async move {
        let io_semaphore = Arc::new(Semaphore::new(2));

        while let Some((path, metadata)) = path_meta_rx.recv().await {
            let metadata = match metadata {
                Ok(metadata) => metadata,
                Err(err) => {
                    eprintln!("failed to read metadata of {}: {}", path, err);
                    continue;
                }
            };

            // Check the database to see if there's anything to recompute.
            let db_metadata = match db.lock().unwrap().get_file(&path) {
                Ok(record) => record,
                Err(err) => {
                    eprintln!("failed to read record for {}, {}", path, err);
                    continue;
                }
            };

            let io_semaphore = io_semaphore.clone();
            let metadata_future = tokio::spawn({
                let db = db.clone();
                process_file(db, io_semaphore, path, metadata, db_metadata)
            });

            if let Err(_) = metadata_tx.send(metadata_future).await {
                eprintln!("receiver dropped");
                return;
            }
        }
    });

    Ok(metadata_rx)
}

pub struct ProcessFileResult {
    pub path: IMPath,
    pub blake3_computed: bool,
    pub content_metadata: ContentMetadata,
    pub image_metadata: Option<ImageMetadata>,
}

async fn process_file(
    db: Arc<Mutex<Database>>,
    io_semaphore: Arc<Semaphore>,
    path: String,
    file_info: FileInfo,
    db_metadata: Option<ContentMetadata>,
) -> Result<ProcessFileResult> {
    let mut blake3_computed = false;

    // TODO: only open the file once, and reuse it for any potential image hashing
    let b3 = match db_metadata {
        Some(ref record) => {
            // If metadata matches our records, we can assume blake3 hasn't changed.
            if file_info == record.file_info {
                record.blake3
            } else {
                blake3_computed = true;
                let _permit = io_semaphore.acquire().await.unwrap();
                // TODO: use into_ok() when it's stabilized
                compute_blake3(PathBuf::from_str(&path).unwrap()).await?
            }
        }
        None => {
            // No record of this file - blake3 must be computed.
            //eprintln!("computing blake3 of {}", path.display());
            blake3_computed = true;
            let _permit = io_semaphore.acquire().await.unwrap();
            // TODO: use into_ok() when it's stabilized
            compute_blake3(PathBuf::from_str(&path).unwrap()).await?
        }
    };

    let content_metadata = ContentMetadata {
        file_info,
        blake3: b3,
    };

    if blake3_computed {
        db.lock()
            .unwrap()
            .add_files(&[(&path, &content_metadata)])?;
    }

    let image_metadata = None;

    /*
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
    */

    Ok(ProcessFileResult {
        path,
        blake3_computed,
        content_metadata,
        image_metadata,
    })
}
