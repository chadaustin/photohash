use walkdir::WalkDir;
use tokio::prelude::*;
use tokio::runtime::Runtime;
use rayon::prelude::*;
use crossbeam::crossbeam_channel::unbounded;
use std::thread;
use futures::channel::oneshot::channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let (paths_sender, paths_receiver) = unbounded();

    rayon::spawn(move || {
        for entry in WalkDir::new(".") {
            if let Ok(e) = entry {
                paths_sender.send(e.into_path());
            }
        }
    });

    let (outputs_sender, outputs_receiver) = unbounded();

    rayon::spawn(move || {
        for path in paths_receiver {
            let (output_sender, output_receiver) = channel();
            outputs_sender.send(output_receiver);
            rayon::spawn(move || {
                output_sender.send(path);
            });
        }
    });

    for output in outputs_receiver {
        println!("{}", output.await?.display());
    }

    Ok(())
}
