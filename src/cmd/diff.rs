use crate::cmd::index;
use crate::Database;
use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::vec::Vec;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "diff", about = "List files not in destination")]
pub struct Diff {
    #[structopt(parse(from_os_str))]
    src: PathBuf,
    #[structopt(parse(from_os_str))]
    dests: Vec<PathBuf>,
}

struct Dots {
    i: u64,
    c: u64,
}

impl Dots {
    fn new() -> Dots {
        Dots { i: 0, c: 100 }
    }

    fn increment(&mut self) {
        self.i += 1;
        if self.i == self.c {
            self.i = 0;
            self.c += self.c >> 4;
            eprint!(".");
        }
    }
}

impl Diff {
    pub async fn run(&self) -> Result<()> {
        let db = Arc::new(Database::open()?);

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
            dst_contents.insert(pfr.content_metadata.blake3, pfr.content_metadata.path);
        }
        eprintln!();

        let mut src_contents = Vec::new();

        while let Some(pfr_future) = src_rx.recv().await {
            let pfr = pfr_future.await??;
            src_contents.push((pfr.content_metadata.path, pfr.content_metadata.blake3));
        }

        src_contents.sort();

        let mut first = true;
        for (path, blake3) in src_contents {
            if !dst_contents.contains_key(&blake3) {
                if first {
                    first = false;
                    eprintln!("Files not in destination:");
                }
                println!("  {}", path.display());
            }
        }

        if first {
            eprintln!("All files exist in destination");
        }

        Ok(())
    }
}
