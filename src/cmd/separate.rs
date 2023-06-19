use crate::cmd::index;
use crate::Database;
use anyhow::bail;
use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::vec::Vec;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "separate", about = "Separate files missing in destination")]
pub struct Separate {
    #[structopt(long, parse(from_os_str))]
    out: PathBuf,
    #[structopt(parse(from_os_str))]
    src: PathBuf,
    #[structopt(parse(from_os_str), required(true))]
    dests: Vec<PathBuf>,
}

impl Separate {
    pub async fn run(&self) -> Result<()> {
        let db = Arc::new(Mutex::new(Database::open()?));

        // To avoid accidentally changing the source directory
        // structure, disallow placing out in src.
        if self.out.starts_with(&self.src) {
            bail!("May not place output within source");
        }

        /*

                // Begin indexing the source in parallel.
                // TODO: If we could guarantee the output channel is sorted, we could
                // incrementally display results.
                let mut src_rx = index::do_index(&db, vec![self.src.clone()])?;

                let mut dst_contents = HashMap::new();

                eprint!("scanning destination...");
                let mut dots = Dots::new();
                let mut dst_rx = index::do_index(&db, self.dests.clone())?;
                while let Some(pfr_future) = dst_rx.recv().await {
                    let pfr = pfr_future.await??;
                    dots.increment();
                    dst_contents.insert(pfr.content_metadata.blake3, pfr.path);
                }
                eprintln!();

                let mut src_contents = Vec::new();

                while let Some(pfr_future) = src_rx.recv().await {
                    let pfr = pfr_future.await??;
                    src_contents.push((pfr.path, pfr.content_metadata.blake3));
                }

                src_contents.sort();

                let mut first = true;
                for (path, blake3) in src_contents {
                    if !dst_contents.contains_key(&blake3) {
                        if first {
                            first = false;
                            eprintln!("Files not in destination:");
                        }
                        println!("  {}", path);
                    }
                }

                if first {
                    eprintln!("All files exist in destination");
                }
        */
        Ok(())
    }
}
