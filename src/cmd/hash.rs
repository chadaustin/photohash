use crate::hash::compute_image_hashes;
use hex::ToHex;
use imagehash::model;
use std::fmt::Display;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(about = "Manually compute hashes for specified files")]
pub struct Hash {
    #[structopt(required(true))]
    files: Vec<PathBuf>,
}

impl Hash {
    pub async fn run(&self) -> anyhow::Result<()> {
        for file in &self.files {
            println!("{}", file.display());
            println!(
                "  blake3:       {}",
                hash_str(crate::compute_blake3(file.clone()).await.map(Some))
            );
            let h = compute_image_hashes(file).await;
            println!(
                "  jpeg_rothash: {}",
                hash_str(h.as_ref().map(|h| h.jpegrothash))
            );
            println!(
                "  blockhash256: {}",
                hash_str(h.as_ref().map(|h| h.blockhash256))
            );
            println!();
        }
        Ok(())
    }
}

fn hash_str<const N: usize, E: Display>(result: Result<Option<model::Hash<N>>, E>) -> String {
    match result {
        Ok(Some(h)) => h.encode_hex(),
        Ok(None) => "missing".to_string(),
        Err(e) => e.to_string(),
    }
}
