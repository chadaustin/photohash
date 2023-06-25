use anyhow::Result;
use hex::ToHex;
use imagehash::model::Hash32;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(about = "Manually compute hashes for specified files")]
pub struct Hash {
    #[structopt(required(true))]
    files: Vec<PathBuf>,
}

impl Hash {
    pub async fn run(&self) -> Result<()> {
        for file in &self.files {
            println!("{}", file.display());
            println!(
                "  blake3:       {}",
                hash_str(crate::compute_blake3(file.clone()).await)
            );
            println!(
                "  jpeg_rothash: {}",
                hash_str(crate::jpeg_rothash(file.clone()).await)
            );
        }
        Ok(())
    }
}

fn hash_str(result: Result<Hash32>) -> String {
    match result {
        Ok(h) => h.encode_hex(),
        Err(e) => e.to_string(),
    }
}
