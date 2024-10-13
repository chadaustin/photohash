use anyhow::Result;
use clap::Args;
use std::path::Path;
use std::path::PathBuf;

fn read_exif_from_jpeg(path: &Path) -> Result<exif::Exif> {
    let file = std::fs::File::open(path)?;

    let reader = exif::Reader::new();
    Ok(reader.read_from_container(&mut std::io::BufReader::new(&file))?)
}

#[derive(Debug, Args)]
#[command(about = "Print EXIF data for a file")]
pub struct Exif {
    #[arg(required = true)]
    paths: Vec<PathBuf>,
}

impl Exif {
    pub async fn run(&self) -> Result<()> {
        for path in &self.paths {
            let exif = read_exif_from_jpeg(path)?;
            println!("{}", path.display());
            for f in exif.fields() {
                println!("  {} {} {}", f.tag, f.ifd_num, f.display_value());
            }
            println!();
        }
        Ok(())
    }
}
