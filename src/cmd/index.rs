use clap::Args;
use photohash::awake;
use photohash::index::do_index;
use photohash::output::select_output_mode;
use photohash::Database;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

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

        let mut output = select_output_mode(self.json);
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
