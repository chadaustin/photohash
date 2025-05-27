use super::index::PixelSemaphore;
use clap::Args;
use photohash::awake;
use photohash::hash::compute_blake3;
use photohash::hash::compute_image_hashes;
use photohash::model::IMPath;
use photohash::scan;
use photohash::Database;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::task::JoinHandle;

#[derive(Args)]
#[command(about = "Validate that files in directories match previously-indexed hashes")]
pub struct Validate {
    #[arg(required = true)]
    srcs: Vec<PathBuf>,
}

impl Validate {
    pub async fn run(mut self) -> anyhow::Result<()> {
        self.srcs = self
            .srcs
            .iter()
            .map(|p| p.canonicalize())
            .collect::<std::io::Result<Vec<PathBuf>>>()?;

        let db = Arc::new(Mutex::new(Database::open()?));

        let dirs: Vec<&Path> = self.srcs.iter().map(|p| p.as_ref()).collect();
        let results = do_validate(&db, &dirs)?;

        let _awake = awake::keep_awake("validating files");

        let mut failures: usize = 0;

        for vr in results {
            let vr = vr.await??;
            if let Some(vr) = vr {
                println!("{} validation failure: {:?}", vr.path, vr.reason);
                failures += 1;
            }
        }

        if failures > 0 {
            anyhow::bail!("{failures} validation failures");
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
struct ValidateResult {
    path: IMPath,
    reason: ValidationReason,
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
enum ValidationReason {
    Blake3Mismatch,
    DimensionsMismatch,
    JpegRothashMismatch,
    Blockhash256Mismatch,
}

fn do_validate(
    db: &Arc<Mutex<Database>>,
    dirs: &[&Path],
) -> anyhow::Result<mpsc::Receiver<JoinHandle<anyhow::Result<Option<ValidateResult>>>>> {
    let scanner = scan::get_default_scan();
    let path_meta_rx = scanner(dirs)?;

    let (validate_tx, validate_rx) = mpsc::channel();

    let db = db.clone();
    tokio::spawn(async move {
        let pixel_semaphore = PixelSemaphore::new().await;

        while let Some((path, _metadata)) = path_meta_rx.recv().await {
            // TODO: validate stored metadata too?
            /*
            let metadata = match metadata {
                Ok(metadata) => metadata,
                Err(err) => {
                    // TODO: propagate error
                    eprintln!("failed to read metadata of {}: {}", path, err);
                    continue;
                }
            };
             */

            let validate_future = tokio::spawn({
                let db = db.clone();
                let pixel_semaphore = pixel_semaphore.clone();
                let path = path.to_owned();
                async move { validate_file(&db, pixel_semaphore, &path).await }
            });
            #[allow(clippy::redundant_pattern_matching)]
            if let Err(_) = validate_tx.send(validate_future) {
                eprintln!("receiver dropped?");
                // receiver dropped
            }
        }
    });

    Ok(validate_rx)
}

async fn validate_file(
    db: &Arc<Mutex<Database>>,
    pixel_semaphore: PixelSemaphore,
    path: &str,
) -> anyhow::Result<Option<ValidateResult>> {
    // Read from the database
    let content_metadata = db.lock().unwrap().get_file(path)?;
    let Some(content_metadata) = content_metadata else {
        // No record of this path implies no need to validate.
        return Ok(None);
    };

    // This is not the correct place to acquire the pixel semaphore.
    // But if we don't limit blake3 as well as the image hashes, a
    // bunch of parallel tasks read the file contents, then block on
    // the semaphore, and then have to reread it after it's out of
    // cache.
    let _permit = pixel_semaphore.acquire().await;

    let blake3 = compute_blake3(PathBuf::from(path)).await?;
    if blake3 != content_metadata.blake3 {
        return Ok(Some(ValidateResult {
            path: path.to_owned(),
            reason: ValidationReason::Blake3Mismatch,
        }));
    }

    let image_metadata = db
        .lock()
        .unwrap()
        .get_image_metadata(&content_metadata.blake3)?;
    let Some(image_metadata) = image_metadata else {
        // BLAKE3 matches and no additional image metadata to validate.
        return Ok(None);
    };

    if image_metadata.dimensions.is_none()
        && image_metadata.jpegrothash.is_none()
        && image_metadata.blockhash256.is_none()
    {
        // No need to compute any image hashes.
        println!("no image metadata: {path}");
        return Ok(None);
    }

    let image_hashes = compute_image_hashes(path).await?;

    if image_hashes.dimensions != image_metadata.dimensions {
        return Ok(Some(ValidateResult {
            path: path.to_owned(),
            reason: ValidationReason::DimensionsMismatch,
        }));
    }

    if image_hashes.jpegrothash != image_metadata.jpegrothash {
        return Ok(Some(ValidateResult {
            path: path.to_owned(),
            reason: ValidationReason::JpegRothashMismatch,
        }));
    }

    if image_hashes.blockhash256 != image_metadata.blockhash256 {
        return Ok(Some(ValidateResult {
            path: path.to_owned(),
            reason: ValidationReason::Blockhash256Mismatch,
        }));
    }

    Ok(None)
}

/*
#[derive(Default)]
struct FoundMatches {
    results: Vec<(String, BTreeSet<String>)>,
}

async fn find_matching(
    db: &Arc<Mutex<Database>>,
    src: PathBuf,
    dests: Vec<PathBuf>,
    exact: bool,
) -> anyhow::Result<FoundMatches> {
    // Begin indexing the source in parallel.
    // TODO: If we could guarantee the output channel is sorted, we could
    // incrementally display results.
    let mut src_rx = index::do_index(db, &[&src], false)?;

    // Scan the destination(s) and build hash tables.
    let index =
        index_destination(db, &dests.iter().map(|p| p.as_ref()).collect::<Vec<_>>()).await?;

    let mut matches = FoundMatches::default();
    while let Some(pfr_future) = src_rx.recv().await {
        let pfr = pfr_future.await??;
        let (path, blake3) = (&pfr.path, pfr.content_metadata.blake3);

        // It would be nice if push returned a &mut to the element.
        matches.results.push((path.clone(), BTreeSet::new()));
        let path_matches = matches.results.last_mut().unwrap();
        if let Some(paths) = index.by_contents.get(&blake3) {
            path_matches.1.extend(paths.iter().cloned());
        }

        if !exact {
            if let Some(mut paths) = pfr
                .jpegrothash()
                .and_then(|rh| index.by_jpegrothash.get(rh))
                .map(|paths| paths.clone().into_iter().collect())
            {
                path_matches.1.append(&mut paths);
            }
            if let Some(mut paths) = pfr
                .blockhash256()
                .and_then(|bh| index.by_blockhash.get(bh))
                .map(|paths| paths.clone().into_iter().collect())
            {
                path_matches.1.append(&mut paths);
            }
        }
    }

    matches.results.sort();
    Ok(matches)
}
 */
