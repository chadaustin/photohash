use crate::compute_blake3;
use anyhow::Result;
use hex::ToHex;
use imagehash::hash;
use imagehash::model::ContentMetadata;
use imagehash::model::FileInfo;
use imagehash::model::Hash32;
use imagehash::model::IMPath;
use imagehash::model::ImageMetadata;
use imagehash::scan;
use imagehash::Database;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use structopt::StructOpt;
use tokio::runtime::Handle;
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

        let here = Path::new(".");
        let dirs: Vec<&Path> = if self.dirs.is_empty() {
            vec![here]
        } else {
            self.dirs.iter().map(|p| p.as_ref()).collect()
        };

        let mut metadata_rx = do_index(&db, &dirs)?;

        let _awake = match keepawake::Builder::new()
            .display(false)
            .idle(true)
            .sleep(true)
            .reason("indexing files")
            .app_name("imagehash")
            .app_reverse_domain("me.chadaustin.imagehash")
            .create()
        {
            Ok(awake) => Some(awake),
            Err(e) => {
                eprintln!("WARNING: keepawake failed, ignoring: {}", e);
                None
            }
        };

        while let Some(content_metadata_future) = metadata_rx.recv().await {
            let content_metadata_future = content_metadata_future.await?;
            let pfr = match content_metadata_future {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("failed to read the thing {}", e);
                    continue;
                }
            };
            let content_metadata = pfr.content_metadata;
            if pfr.blake3_computed || pfr.image_metadata_computed {
                println!(
                    "{} {:>8}K {}",
                    content_metadata.blake3.encode_hex::<String>(),
                    content_metadata.file_info.size / 1024,
                    dunce::simplified(pfr.path.as_ref()).display(),
                );
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
    dirs: &[&Path],
) -> Result<mpsc::Receiver<JoinHandle<Result<ProcessFileResult>>>> {
    let scanner = scan::get_default_scan();
    let path_meta_rx = scanner(dirs)?;

    let (metadata_tx, metadata_rx) = mpsc::channel(RESULT_CHANNEL_SIZE);

    // Reads enumerated paths and computes necessary file metadata and content hashes.
    let db = db.clone();
    tokio::spawn(async move {
        // Limit the number of concurrent perceptual hashes, since
        // keeping pixel data in RAM is expensive.
        //
        // TODO: To actually bound memory usage while maximizing CPU
        // utilization, use a semaphore with number-of-pixels count
        // and acquire width*height permits after reading the image
        // header.
        let pixel_semaphore_count = 4 * Handle::current().metrics().num_workers();
        let pixel_semaphore = Arc::new(Semaphore::new(pixel_semaphore_count));

        while let Some((path, metadata)) = path_meta_rx.recv().await {
            let metadata = match metadata {
                Ok(metadata) => metadata,
                Err(err) => {
                    // TODO: propagate error
                    eprintln!("failed to read metadata of {}: {}", path, err);
                    continue;
                }
            };

            // Check the database to see if there's anything to recompute.
            let db_metadata = match db.lock().unwrap().get_file(&path) {
                Ok(record) => record,
                Err(err) => {
                    // TODO: propagate error
                    eprintln!("failed to read record for {}, {}", path, err);
                    continue;
                }
            };

            let metadata_future = tokio::spawn({
                let db = db.clone();
                let pixel_semaphore = pixel_semaphore.clone();
                process_file(db, pixel_semaphore, path, metadata, db_metadata)
            });

            if let Err(_) = metadata_tx.send(metadata_future).await {
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
    pub image_metadata_computed: bool,
    pub image_metadata: Option<ImageMetadata>,
}

impl ProcessFileResult {
    pub fn blockhash256(&self) -> Option<&Hash32> {
        self.image_metadata
            .as_ref()
            .and_then(|im| im.blockhash256.as_ref())
    }

    pub fn jpegrothash(&self) -> Option<&Hash32> {
        self.image_metadata
            .as_ref()
            .and_then(|im| im.jpegrothash.as_ref())
    }
}

async fn process_file(
    db: Arc<Mutex<Database>>,
    pixel_semaphore: Arc<Semaphore>,
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
                compute_blake3(PathBuf::from(&path)).await?
            }
        }
        None => {
            // No record of this file - blake3 must be computed.
            //eprintln!("computing blake3 of {}", path.display());
            blake3_computed = true;
            compute_blake3(PathBuf::from(&path)).await?
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

    let mut image_metadata_computed = false;
    let mut image_metadata = None;
    if hash::may_have_metadata(&path, &content_metadata.file_info) {
        image_metadata = db.lock().unwrap().get_image_metadata(&b3)?;
        if image_metadata.is_none() {
            let _permit = pixel_semaphore.clone().acquire_owned().await?;

            match hash::compute_image_hashes(&path).await {
                Ok(im) => {
                    db.lock()
                        .unwrap()
                        .add_image_metadata(&content_metadata.blake3, &im)?;
                    image_metadata_computed = true;
                    image_metadata = Some(im);
                }
                Err(hash::ImageMetadataError::UnsupportedPhoto { path, source }) => {
                    match source {
                        Some(source) => {
                            eprintln!("invalid photo {}: {:#?}", path.display(), source);
                        }
                        None => {
                            eprintln!("invalid photo {}: unknown reason", path.display());
                        }
                    }
                    // TODO: record in the database this is an invalid photo
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    }

    Ok(ProcessFileResult {
        path,
        blake3_computed,
        content_metadata,
        image_metadata_computed,
        image_metadata,
    })
}
