use anyhow::Result;
use std::sync::Arc;
use crate::Database;
use structopt::StructOpt;
use std::path::PathBuf;
use crate::cmd::index;

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
        Dots {
            i: 0,
            c: 100,
        }
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

        eprint!("scanning destination...");
        let mut dots = Dots::new();
        let mut dst_rx = index::do_index(&db, self.dests.clone());
        while let Some(content_metadata_future) = dst_rx.recv().await {
            let _ = content_metadata_future.await??;
            dots.increment();
        }
        eprintln!();



/*
        let src_scan_result = collect_scan(&[self.src]);

        for entry in src_scan_result {
            if entry not in dst_scan_result {
                println!("{}", entry.path().relative_to(here).display());
            }
        }
*/
        Ok(())
    }
}
