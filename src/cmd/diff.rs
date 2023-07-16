use crate::cmd::index;
use anyhow::Result;
use imagehash::model::Hash32;
use imagehash::model::IMPath;
use imagehash::Database;
use std::collections::BTreeMap;
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
    /// Disregard perceptual hashes and only list files whose exact contents aren't in destination
    #[structopt(long)]
    exact: bool,

    /// For all files that exist in destination, display corresponding paths
    #[structopt(long)]
    matches: bool,

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
            // Decelerate over time. Acceleration is nicer but we
            // don't know how many files there are.
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

        if self.matches {
            Self::print_matches("Exact matches:", difference.matches_by_contents);
            Self::print_matches(
                "Matches by JPEG rotation hash:",
                difference.matches_by_jpegrothash,
            );
            Self::print_matches(
                "Matches by perceptual hash:",
                difference.matches_by_blockhash,
            );
        }

        let missing = &difference.missing;
        if missing.is_empty() {
            println!("All files exist in destination");
            return Ok(());
        }

        println!("Files not in destination:");
        for path in missing {
            println!("  {}", path);
        }

        Ok(())
    }

    fn print_matches(label: &str, matches: BTreeMap<IMPath, Vec<IMPath>>) {
        if matches.is_empty() {
            return;
        }

        println!("{label}\n");
        for (path, mut matching) in matches {
            matching.sort();
            let path = dunce::simplified(path.as_ref()).display();
            if matching.len() == 1 {
                println!(
                    "{} -> {}",
                    path,
                    dunce::simplified(matching[0].as_ref()).display()
                );
            } else {
                println!("{} ->", path);
                for m in matching {
                    println!("  {}", dunce::simplified(m.as_ref()).display());
                }
            }
        }
        println!();
    }
}

#[derive(Default)]
pub struct Differences {
    pub missing: Vec<IMPath>,

    pub matches_by_contents: BTreeMap<IMPath, Vec<IMPath>>,
    pub matches_by_jpegrothash: BTreeMap<IMPath, Vec<IMPath>>,
    pub matches_by_blockhash: BTreeMap<IMPath, Vec<IMPath>>,
}

pub async fn compute_difference(
    db: &Arc<Mutex<Database>>,
    src: PathBuf,
    dests: Vec<PathBuf>,
    exact: bool,
) -> Result<Differences> {
    // Begin indexing the source in parallel.
    // TODO: If we could guarantee the output channel is sorted, we could
    // incrementally display results.
    let mut src_rx = index::do_index(db, &[&src])?;

    // blake3 -> {path}
    let mut by_contents: HashMap<Hash32, HashSet<IMPath>> = HashMap::new();
    // jpegrothash -> {path}
    let mut by_jpegrothash: HashMap<Hash32, HashSet<IMPath>> = HashMap::new();
    // blockhash256 -> {path}
    let mut by_blockhash: HashMap<Hash32, HashSet<IMPath>> = HashMap::new();

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
        if let Some(rh) = pfr.jpegrothash() {
            by_jpegrothash.entry(*rh).or_default().insert(path.clone());
        }
        if let Some(bh) = pfr.blockhash256() {
            by_blockhash.entry(*bh).or_default().insert(path.clone());
        }
    }
    eprintln!();

    let mut differences = Differences::default();
    while let Some(pfr_future) = src_rx.recv().await {
        let pfr = pfr_future.await??;
        let (path, blake3) = (&pfr.path, pfr.content_metadata.blake3);
        if let Some(paths) = by_contents.get(&blake3) {
            differences
                .matches_by_contents
                .entry(path.clone())
                .or_default()
                .append(&mut paths.clone().into_iter().collect());
            continue;
        }

        if !exact {
            if let Some(mut paths) = pfr
                .jpegrothash()
                .and_then(|rh| by_jpegrothash.get(rh))
                .map(|paths| paths.clone().into_iter().collect())
            {
                differences
                    .matches_by_jpegrothash
                    .entry(path.clone())
                    .or_default()
                    .append(&mut paths);
                continue;
            }
            if let Some(mut paths) = pfr
                .blockhash256()
                .and_then(|bh| by_blockhash.get(bh))
                .map(|paths| paths.clone().into_iter().collect())
            {
                differences
                    .matches_by_blockhash
                    .entry(path.clone())
                    .or_default()
                    .append(&mut paths);
                continue;
            }
        }

        differences.missing.push(path.clone());
    }

    differences.missing.sort();

    Ok(differences)
}
