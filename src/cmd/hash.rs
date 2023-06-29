use anyhow::Result;
use hex::ToHex;
use imagehash::model;
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
            println!(
                "  blockhash256: {}",
                hash_str(
                    crate::perceptual_hash(file.clone())
                        .await
                        .map(|e| e.blockhash256)
                )
            );
            println!();
        }
        Ok(())
    }
}

fn hash_str<const N: usize>(result: Result<model::Hash<N>>) -> String {
    match result {
        Ok(h) => h.encode_hex(),
        Err(e) => e.to_string(),
    }
}
