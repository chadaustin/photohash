use anyhow::Context;
use clap::Args;
use hex::ToHex;
use photohash::awake;
use photohash::hash;
use photohash::hash::update_content_hashes;
use photohash::hash::ContentHashSet;
use photohash::hash::ContentHashType;
use photohash::hash::ContentHashes;
use photohash::index::ProcessFileResult;
use photohash::model::ContentMetadata;
use photohash::model::FileInfo;
use photohash::model::ImageMetadata;
use photohash::scan;
use photohash::Database;
use serde::ser::SerializeSeq;
use serde::Serialize;
use serde::Serializer;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use superconsole::SuperConsole;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

// TODO: Introduce PhotohashConfig
const RESULT_CHANNEL_SIZE: usize = 8;
const TRACE_INVALID_PHOTOS: bool = false;

#[derive(Args)]
#[command(name = "index", about = "Scan directories and update the index")]
pub struct Index {
    #[arg(long)]
    json: bool,

    #[arg(long)]
    extra_hashes: bool,

    #[arg(required(true))]
    dirs: Vec<PathBuf>,
}

#[derive(Serialize)]
struct JsonRecord {
    path: String,
    size: u64,
    blake3: String,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    extra_hashes: Option<JsonExtraHashes>,
}

#[derive(Serialize)]
struct JsonExtraHashes {
    #[serde(skip_serializing_if = "Option::is_none")]
    md5: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sha1: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sha256: Option<String>,
}

trait OutputMode {
    fn start(&mut self) -> anyhow::Result<Box<dyn OutputSequence<'_> + '_>>;
}

trait OutputSequence<'a> {
    fn file(&mut self, pfr: &ProcessFileResult) -> anyhow::Result<()>;
    fn end(self: Box<Self>) -> anyhow::Result<()>;
}

struct StdoutMode;

impl OutputMode for StdoutMode {
    fn start(&mut self) -> anyhow::Result<Box<dyn OutputSequence<'_>>> {
        // Does not allocate because StdoutMode is ZST.
        Ok(Box::new(StdoutSequence))
    }
}

struct StdoutSequence;

impl OutputSequence<'_> for StdoutSequence {
    fn file(&mut self, pfr: &ProcessFileResult) -> anyhow::Result<()> {
        if pfr.blake3_computed || pfr.image_metadata_computed {
            let content_metadata = &pfr.content_metadata;
            println!(
                "{} {:>8}K {}",
                content_metadata.blake3.encode_hex::<String>(),
                content_metadata.file_info.size / 1024,
                &dunce::simplified(pfr.path.as_ref()).display(),
            );
        }
        Ok(())
    }

    fn end(self: Box<Self>) -> anyhow::Result<()> {
        Ok(())
    }
}

struct TtyMode(Option<SuperConsole>);
struct TtySequence(SuperConsole);

impl OutputMode for TtyMode {
    fn start(&mut self) -> anyhow::Result<Box<dyn OutputSequence<'_>>> {
        Ok(Box::new(TtySequence(
            self.0.take().expect("cannot start TtyMode twice"),
        )))
    }
}

impl OutputSequence<'_> for TtySequence {
    fn file(&mut self, pfr: &ProcessFileResult) -> anyhow::Result<()> {
        let sc = &mut self.0;
        let content_metadata = &pfr.content_metadata;
        use superconsole::components::Blank;
        use superconsole::Line;
        use superconsole::Lines;
        if pfr.blake3_computed || pfr.image_metadata_computed {
            sc.emit(Lines(vec![Line::unstyled(&format!(
                "{} {:>8}K {}",
                content_metadata.blake3.encode_hex::<String>(),
                content_metadata.file_info.size / 1024,
                &dunce::simplified(pfr.path.as_ref()).display(),
            ))?]));
            sc.render(&Blank)?;
        }
        Ok(())
    }

    fn end(self: Box<Self>) -> anyhow::Result<()> {
        self.0.finalize(&superconsole::components::Blank)?;
        Ok(())
    }
}

struct JsonMode {
    serializer: serde_json::Serializer<std::io::Stdout>,
}
struct JsonSequence<'a>(
    Option<<&'a mut serde_json::Serializer<std::io::Stdout> as serde::Serializer>::SerializeSeq>,
);

impl JsonMode {
    fn new() -> Self {
        let serializer = serde_json::Serializer::new(std::io::stdout());
        Self { serializer }
    }
}

impl OutputMode for JsonMode {
    fn start(&mut self) -> anyhow::Result<Box<dyn OutputSequence<'_> + '_>> {
        Ok(Box::new(JsonSequence(Some(
            self.serializer.serialize_seq(None)?,
        ))))
    }
}

impl OutputSequence<'_> for JsonSequence<'_> {
    fn file(&mut self, pfr: &ProcessFileResult) -> anyhow::Result<()> {
        let content_metadata = &pfr.content_metadata;
        self.0.as_mut().unwrap().serialize_element(&JsonRecord {
            path: pfr.path.clone(),
            size: content_metadata.file_info.size,
            blake3: hex::encode(content_metadata.blake3),
            extra_hashes: pfr.extra_hashes.as_ref().map(|eh| JsonExtraHashes {
                md5: eh.md5.map(hex::encode),
                sha1: eh.sha1.map(hex::encode),
                sha256: eh.sha256.map(hex::encode),
            }),
        })?;
        Ok(())
    }

    fn end(mut self: Box<Self>) -> anyhow::Result<()> {
        self.0.take().unwrap().end()?;
        Ok(())
    }
}

impl Index {
    pub async fn run(&self) -> anyhow::Result<()> {
        let db = Arc::new(Mutex::new(Database::open()?));

        let here = Path::new(".");
        let dirs: Vec<&Path> = if self.dirs.is_empty() {
            vec![here]
        } else {
            self.dirs.iter().map(|p| p.as_ref()).collect()
        };

        let mut metadata_rx = do_index(&db, &dirs, self.extra_hashes)?;

        let _awake = awake::keep_awake("indexing files");

        let mut output: Box<dyn OutputMode> = if self.json {
            Box::new(JsonMode::new())
        } else if let Some(sc) = SuperConsole::new() {
            Box::new(TtyMode(Some(sc)))
        } else {
            Box::new(StdoutMode)
        };
        let mut seq = output.start()?;

        while let Some(content_metadata_future) = metadata_rx.recv().await {
            let content_metadata_future = content_metadata_future.await?;
            let pfr = match content_metadata_future {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("failed to read the thing {}", e);
                    continue;
                }
            };

            seq.file(&pfr)?;
        }

        seq.end()?;

        Ok(())
    }
}

// Limit the number of concurrent perceptual hashes, since
// keeping pixel data in RAM is expensive.
#[derive(Clone)]
pub struct PixelSemaphore {
    inner: Arc<Semaphore>,
}

impl PixelSemaphore {
    pub async fn new() -> Self {
        // TODO: To actually bound memory usage while maximizing CPU
        // utilization, use a semaphore with number-of-pixels count
        // and acquire width*height permits after reading the image
        // header.
        let count = 4 * Handle::current().metrics().num_workers();
        Self {
            inner: Arc::new(Semaphore::new(count)),
        }
    }

    #[cfg(test)]
    pub fn test() -> Self {
        Self {
            inner: Arc::new(Semaphore::new(1)),
        }
    }

    pub async fn acquire(self) -> OwnedSemaphorePermit {
        self.inner
            .acquire_owned()
            .await
            .expect("Pixel semaphores are never closed")
    }
}

pub fn do_index(
    db: &Arc<Mutex<Database>>,
    dirs: &[&Path],
    compute_extra_hashes: bool,
) -> anyhow::Result<mpsc::Receiver<JoinHandle<anyhow::Result<ProcessFileResult>>>> {
    let scanner = scan::get_default_scan();
    let path_meta_rx = scanner(dirs)?;

    let (metadata_tx, metadata_rx) = mpsc::channel(RESULT_CHANNEL_SIZE);

    // Reads enumerated paths and computes necessary file metadata and content hashes.
    let db = db.clone();
    tokio::spawn(async move {
        let pixel_semaphore = PixelSemaphore::new().await;

        while let Some((path, metadata)) = path_meta_rx.recv().await {
            let metadata = match metadata {
                Ok(metadata) => metadata,
                Err(err) => {
                    // TODO: propagate error
                    eprintln!("failed to read metadata of {}: {}", path, err);
                    continue;
                }
            };

            let metadata_future = tokio::spawn({
                let db = db.clone();
                let pixel_semaphore = pixel_semaphore.clone();
                process_file(db, pixel_semaphore, path, metadata, compute_extra_hashes)
            });

            let Ok(()) = metadata_tx.send(metadata_future).await else {
                // Receiver stopped listening: abort.
                return;
            };
        }
    });

    Ok(metadata_rx)
}

async fn process_file(
    db: Arc<Mutex<Database>>,
    pixel_semaphore: PixelSemaphore,
    path: String,
    file_info: FileInfo,
    compute_extra_hashes: bool,
) -> anyhow::Result<ProcessFileResult> {
    // TODO: This all needs unit tests.

    // Check the database to see if there's anything to recompute.
    let db_metadata = db
        .lock()
        .unwrap()
        .get_file(&path)
        .with_context(|| format!("failed to read record for {}", path))?;

    let mut current_hashes = ContentHashes::default();
    if db_metadata.as_ref().map(|m| m.file_info == file_info) == Some(true) {
        // Existing hashes are valid.
        current_hashes.blake3 = db_metadata.map(|m| m.blake3);
    } else {
        // Either we haven't indexed this file or the database's
        // recorded blake3 does not match the current file.
    }

    // Do we have saved extra hashes?
    if compute_extra_hashes {
        if let Some(b3) = current_hashes.blake3 {
            if let Some(extra_hashes) =
                db.lock().unwrap().get_extra_hashes(&b3).with_context(|| {
                    format!(
                        "failed to read extra hashes for {}: {}",
                        path,
                        hex::encode(b3)
                    )
                })?
            {
                current_hashes.extra_hashes = extra_hashes;
            }
        }
    }

    // We always want blake3.
    let mut desired_hashes = ContentHashSet::only(ContentHashType::BLAKE3);
    if compute_extra_hashes {
        desired_hashes |= ContentHashType::MD5 | ContentHashType::SHA1 | ContentHashType::SHA256;
    }

    // TODO: Only open the file once, and reuse it for any potential
    // image hashing.
    let computed_hashes =
        update_content_hashes(&mut current_hashes, PathBuf::from(&path), desired_hashes).await?;
    let b3 = current_hashes
        .blake3
        .expect("invariant: blake3 is always requested");

    let content_metadata = ContentMetadata {
        file_info,
        blake3: b3,
    };

    let blake3_computed = computed_hashes.contains(ContentHashType::BLAKE3);
    if blake3_computed {
        db.lock()
            .unwrap()
            .add_files(&[(&path, &content_metadata)])?;
    }

    let extra_hashes_computed = computed_hashes
        .is_superset(ContentHashType::MD5 | ContentHashType::SHA1 | ContentHashType::SHA256);
    if extra_hashes_computed {
        db.lock()
            .unwrap()
            .add_extra_hashes(&b3, &current_hashes.extra_hashes)?;
    }

    let mut image_metadata_computed = false;
    let mut image_metadata = None;
    if hash::may_have_metadata(&path, &content_metadata.file_info) {
        image_metadata = db.lock().unwrap().get_image_metadata(&b3)?;
        if image_metadata.is_none() {
            let _permit = pixel_semaphore.clone().acquire().await;

            match hash::compute_image_hashes(&path).await {
                Ok(im) => {
                    image_metadata_computed = true;
                    image_metadata = Some(im);
                }
                Err(hash::ImageMetadataError::UnsupportedPhoto { path, source }) => {
                    if TRACE_INVALID_PHOTOS {
                        // TODO: Should we forward the source with the error here?
                        match source {
                            Some(source) => {
                                eprintln!("invalid photo {}: {:?}", path.display(), source);
                            }
                            None => {
                                eprintln!("invalid photo {}: unknown reason", path.display());
                            }
                        }
                    }
                    // Record in the database this is an invalid photo.
                    image_metadata_computed = true;
                    image_metadata = Some(ImageMetadata::invalid());
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    }

    if image_metadata_computed {
        db.lock()
            .unwrap()
            .add_image_metadata(&content_metadata.blake3, image_metadata.as_ref().unwrap())?;
    }

    let has_extra_hashes = current_hashes.extra_hashes.md5.is_some()
        && current_hashes.extra_hashes.sha1.is_some()
        && current_hashes.extra_hashes.sha256.is_some();

    Ok(ProcessFileResult {
        path,
        blake3_computed,
        content_metadata,
        image_metadata_computed,
        image_metadata,
        extra_hashes: if has_extra_hashes {
            Some(current_hashes.extra_hashes)
        } else {
            None
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_computes_blake3() -> anyhow::Result<()> {
        let db = Arc::new(Mutex::new(Database::open_memory()?));
        let pixel_semaphore = PixelSemaphore::test();
        let file_info = FileInfo {
            inode: 1,
            size: 2,
            mtime: SystemTime::now(),
        };

        let pfr = process_file(
            db,
            pixel_semaphore,
            String::from("tests/images/Moonlight.heic"),
            file_info,
            false,
        )
        .await?;
        assert_eq!(
            "d8828886771faa4da22c36c352acdbf0988f780b457dd8525499a3f2153a25d5",
            hex::encode(pfr.content_metadata.blake3)
        );
        assert_eq!(None, pfr.extra_hashes);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_file_extra_hashes_twice() -> anyhow::Result<()> {
        let db = Arc::new(Mutex::new(Database::open_memory()?));
        let pixel_semaphore = PixelSemaphore::test();
        let file_info = FileInfo {
            inode: 1,
            size: 2,
            mtime: SystemTime::now(),
        };

        let pfr = process_file(
            db.clone(),
            pixel_semaphore.clone(),
            "tests/images/Moonlight.heic".to_owned(),
            file_info.clone(),
            true,
        )
        .await?;
        assert_eq!(
            "d8828886771faa4da22c36c352acdbf0988f780b457dd8525499a3f2153a25d5",
            hex::encode(pfr.content_metadata.blake3)
        );
        let extra_hashes = pfr.extra_hashes.as_ref();
        assert_eq!(
            Some("e5dae7611472d7102fc3a05a16152247".to_owned()),
            extra_hashes.and_then(|eh| eh.md5).map(hex::encode)
        );
        assert_eq!(
            Some("3260706646db71bb48cc4165e46410fde3e98a44".to_owned()),
            extra_hashes.and_then(|eh| eh.sha1).map(hex::encode)
        );
        assert_eq!(
            Some("d5b1055e3a5f5fc68d5a1ae706639f3cd1bd34349e6db7003e48cb11f755d3e8".to_owned()),
            extra_hashes.and_then(|eh| eh.sha256).map(hex::encode)
        );

        // We should have saved this information in the database. Look it up again.
        let pfr = process_file(
            db,
            pixel_semaphore,
            "tests/images/Moonlight.heic".to_owned(),
            file_info,
            true,
        )
        .await?;
        assert_eq!(
            "d8828886771faa4da22c36c352acdbf0988f780b457dd8525499a3f2153a25d5",
            hex::encode(pfr.content_metadata.blake3)
        );
        let extra_hashes = pfr.extra_hashes.as_ref();
        assert_eq!(
            Some("e5dae7611472d7102fc3a05a16152247".to_owned()),
            extra_hashes.and_then(|eh| eh.md5).map(hex::encode)
        );
        assert_eq!(
            Some("3260706646db71bb48cc4165e46410fde3e98a44".to_owned()),
            extra_hashes.and_then(|eh| eh.sha1).map(hex::encode)
        );
        assert_eq!(
            Some("d5b1055e3a5f5fc68d5a1ae706639f3cd1bd34349e6db7003e48cb11f755d3e8".to_owned()),
            extra_hashes.and_then(|eh| eh.sha256).map(hex::encode)
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn index_single_file() -> anyhow::Result<()> {
        let db = Arc::new(Mutex::new(Database::open_memory()?));
        let mut receiver = do_index(&db, &[&Path::new("tests/images/Moonlight.heic")], true)?;
        let pfr: ProcessFileResult = receiver.recv().await.expect("must be one item").await??;
        assert!(pfr.blake3_computed);
        assert_eq!(
            "d8828886771faa4da22c36c352acdbf0988f780b457dd8525499a3f2153a25d5",
            hex::encode(pfr.content_metadata.blake3)
        );
        assert!(receiver.recv().await.is_none());

        //assert_eq!(None, e);
        Ok(())
    }
}
