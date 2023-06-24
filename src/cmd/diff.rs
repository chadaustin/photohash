use crate::cmd::index;
use crate::model::IMPath;
use crate::Database;
use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::vec::Vec;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "diff", about = "List files missing in destination")]
pub struct Diff {
    #[structopt(parse(from_os_str))]
    src: PathBuf,
    #[structopt(parse(from_os_str), required(true))]
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
        let db = Arc::new(Mutex::new(Database::open()?));

        let difference = compute_difference(&db, self.src.clone(), self.dests.clone()).await?;
        if difference.is_empty() {
            eprintln!("All files exist in destination");
            return Ok(());
        }

        eprintln!("Files not in destination:");
        for path in difference {
            println!("  {}", path);
        }

        Ok(())
    }
}

pub async fn compute_difference(
    db: &Arc<Mutex<Database>>,
    src: PathBuf,
    dests: Vec<PathBuf>,
) -> Result<Vec<IMPath>> {
    // Begin indexing the source in parallel.
    // TODO: If we could guarantee the output channel is sorted, we could
    // incrementally display results.
    let mut src_rx = index::do_index(db, vec![src])?;

    let mut dst_contents = HashMap::new();

    eprint!("scanning destination...");
    let mut dots = Dots::new();
    let mut dst_rx = index::do_index(db, dests)?;
    while let Some(pfr_future) = dst_rx.recv().await {
        let pfr = pfr_future.await??;
        dots.increment();
        dst_contents.insert(pfr.content_metadata.blake3, pfr.path);
    }
    eprintln!();

    let mut difference = Vec::new();
    while let Some(pfr_future) = src_rx.recv().await {
        let pfr = pfr_future.await??;
        let (path, blake3) = (pfr.path, pfr.content_metadata.blake3);
        if !dst_contents.contains_key(&blake3) {
            difference.push(path);
        }
    }

    Ok(difference)
}
