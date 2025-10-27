use clap::Args;
use photohash::hash;
use photohash::index::do_index;
use photohash::Database;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::vec::Vec;

#[derive(Debug, Args)]
#[command(name = "broken", about = "List all image files that cannot be decoded")]
pub struct Broken {
    #[arg(required = true)]
    dirs: Vec<PathBuf>,
}

impl Broken {
    pub async fn run(&self) -> anyhow::Result<()> {
        let db = Arc::new(Mutex::new(Database::open()?));

        let dirs: Vec<_> = self.dirs.iter().map(|d| d.as_ref()).collect();

        let mut discrepancy_count: u64 = 0;

        let mut metadata_rx = do_index(&db, &dirs, false)?;
        while let Some(pfr_future) = metadata_rx.recv().await {
            let pfr = pfr_future.await??;
            if let Some(image_metadata) = pfr.image_metadata {
                if image_metadata.is_valid() {
                    continue;
                }

                let path = dunce::simplified(pfr.path.as_ref()).display();
                match hash::compute_image_hashes(&pfr.path).await {
                    Ok(_im) => {
                        println!("{}: recorded as failed photo but looks valid now", path);
                        discrepancy_count += 1;
                    }
                    Err(hash::ImageMetadataError::UnsupportedPhoto { source, .. }) => {
                        if let Some(source) = source {
                            println!("{}", path);
                            for cause in source.chain() {
                                println!("    {}", cause);
                            }
                        } else {
                            println!("{}: invalid photo with missing root cause", path);
                        }
                        discrepancy_count += 1;
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
        }

        if discrepancy_count > 0 {
            anyhow::bail!("{discrepancy_count} discrepancies found");
        }

        Ok(())
    }
}
