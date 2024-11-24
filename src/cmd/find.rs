use crate::cmd::diff::index_destination;
use crate::cmd::index;
use clap::Args;
use photohash::Database;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Args)]
#[command(about = "Find matching files in destination")]
pub struct Find {
    src: PathBuf,

    #[arg(required = true)]
    dests: Vec<PathBuf>,

    /// Disregard perceptual hashes and only list files whose exact contents aren't in destination
    #[arg(long)]
    exact: bool,
}

impl Find {
    pub async fn run(mut self) -> anyhow::Result<()> {
        self.src = self.src.canonicalize()?;
        self.dests = self
            .dests
            .iter()
            .map(|p| p.canonicalize())
            .collect::<std::io::Result<Vec<PathBuf>>>()?;

        let db = Arc::new(Mutex::new(Database::open()?));
        let differences =
            find_matching(&db, self.src.clone(), self.dests.clone(), self.exact).await?;
        for (path, matches) in differences.results {
            println!(
                "{}{}",
                dunce::simplified(path.as_ref()).display(),
                if matches.is_empty() {
                    ": no matches"
                } else {
                    " has matches:"
                },
            );
            for m in matches {
                println!("  {}", dunce::simplified(m.as_ref()).display());
            }
        }

        Ok(())
    }
}

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
    let mut src_rx = index::do_index(db, &[&src])?;

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
