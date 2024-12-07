use clap::Args;
use hex::ToHex;
use photohash::hash::compute_blake3;
use photohash::hash::compute_image_hashes;
use photohash::model;
use std::fmt::Display;
use std::path::PathBuf;

#[derive(Debug, Args)]
#[command(about = "Manually display hashes for specific files")]
pub struct Hash {
    #[arg(required = true)]
    files: Vec<PathBuf>,
}

impl Hash {
    pub async fn run(&self) -> anyhow::Result<()> {
        for file in &self.files {
            println!("{}", file.display());
            println!(
                "  blake3:       {}",
                hash_str(compute_blake3(file.clone()).await.map(Some))
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
