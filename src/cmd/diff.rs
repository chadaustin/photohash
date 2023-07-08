use crate::cmd::index;
use anyhow::Result;
use imagehash::model::Hash32;
use imagehash::model::IMPath;
use imagehash::Database;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::vec::Vec;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "diff", about = "List files missing in destination")]
pub struct Diff {
    #[structopt(long)]
    exact: bool,
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

        let difference =
            compute_difference(&db, self.src.clone(), self.dests.clone(), self.exact).await?;
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
    exact: bool,
) -> Result<Vec<IMPath>> {
    // Begin indexing the source in parallel.
    // TODO: If we could guarantee the output channel is sorted, we could
    // incrementally display results.
    let mut src_rx = index::do_index(db, &[&src])?;

    // blake3 -> {path}
    let mut by_contents: HashMap<Hash32, HashSet<IMPath>> = HashMap::new();
    // blockhash256 -> {path}
    let mut by_blockhash: HashMap<Hash32, HashSet<IMPath>> = HashMap::new();
    // rothash -> {path}
    let mut by_rothash: HashMap<Hash32, HashSet<IMPath>> = HashMap::new();

    // Scan the destination(s) and build hash tables.
    eprint!("scanning destination...");
    let mut dots = Dots::new();
    let mut dst_rx = index::do_index(db, &dests.iter().map(|p| p.as_ref()).collect::<Vec<_>>())?;
    while let Some(pfr_future) = dst_rx.recv().await {
        dots.increment();

        let pfr = pfr_future.await??;
        let path = &pfr.path;

        by_contents
            .entry(pfr.content_metadata.blake3)
            .or_default()
            .insert(path.clone());
        if let Some(bh) = pfr.blockhash256() {
            by_blockhash.entry(*bh).or_default().insert(path.clone());
        }
        if let Some(rh) = pfr.jpegrothash() {
            by_rothash.entry(*rh).or_default().insert(path.clone());
        }
    }
    eprintln!();

    let mut difference = Vec::new();
    while let Some(pfr_future) = src_rx.recv().await {
        let pfr = pfr_future.await??;
        let (path, blake3) = (&pfr.path, pfr.content_metadata.blake3);
        if by_contents.contains_key(&blake3) {
            // TODO: worth printing the originating path(s)?
            continue;
        }

        if !exact {
            if Some(true) == pfr.blockhash256().map(|bh| by_blockhash.contains_key(bh)) {
                // TODO: worth printing the originating path(s)?
                continue;
            }
            if Some(true) == pfr.jpegrothash().map(|rh| by_rothash.contains_key(rh)) {
                // TODO: worth printing the originating path(s)?
                continue;
            }
        }

        difference.push(path.clone());
    }

    Ok(difference)
}
