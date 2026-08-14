use digest::Digest;
use photohash::hash::compute_content_hashes;
use photohash::hash::ContentHashType;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::Write;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Barrier;
use std::time::Duration;
use std::time::Instant;

const FILE_SIZE: u64 = 8 * 1024 * 1024;

#[cfg(unix)]
#[tokio::test]
async fn hashing_an_atomically_replaced_path_again_uses_new_file() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("file");
    let replacement_path = dir.path().join("replacement");
    let old_contents = vec![0x11; FILE_SIZE as usize];
    let final_contents = vec![0x22; FILE_SIZE as usize];
    std::fs::write(&path, &old_contents)?;
    std::fs::write(&replacement_path, &final_contents)?;

    let old_hash: [u8; 32] = sha2::Sha256::digest(&old_contents).into();
    let final_hash: [u8; 32] = sha2::Sha256::digest(&final_contents).into();
    let started = Arc::new(Barrier::new(2));
    let replacer = std::thread::spawn({
        let path = path.clone();
        let started = started.clone();
        move || -> std::io::Result<()> {
            started.wait();
            // Give the hasher time to open the original file. Renaming over its
            // path does not change the metadata visible through that descriptor.
            std::thread::sleep(Duration::from_millis(25));
            std::fs::rename(replacement_path, path)
        }
    });

    started.wait();
    // This hash may legitimately describe the old descriptor even though its
    // path is replaced while it is being read.
    let first_hashes = compute_content_hashes(path.clone(), ContentHashType::SHA256.into()).await?;
    replacer.join().expect("replacer thread panicked")?;
    assert_eq!(Some(old_hash), first_hashes.extra_hashes.sha256);

    // A subsequent indexing pass opens the path again and must see the new file.
    let hashes = compute_content_hashes(path, ContentHashType::SHA256.into()).await?;
    assert_eq!(Some(final_hash), hashes.extra_hashes.sha256);
    Ok(())
}

#[tokio::test]
async fn hashing_retries_if_the_file_changes() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("changing-then-stable-file");
    let old_contents = vec![0x11; FILE_SIZE as usize];
    let final_contents = vec![0x22; FILE_SIZE as usize];
    std::fs::write(&path, &old_contents)?;

    let expected: [u8; 32] = sha2::Sha256::digest(&final_contents).into();
    let started = Arc::new(Barrier::new(2));
    let writer = std::thread::spawn({
        let path = path.clone();
        let started = started.clone();
        let old_contents = old_contents.clone();
        let final_contents = final_contents.clone();
        move || -> std::io::Result<()> {
            let mut file = OpenOptions::new().write(true).open(path)?;
            let start = Instant::now();
            let mut write_old = false;
            started.wait();
            while start.elapsed() < Duration::from_millis(50) {
                file.rewind()?;
                file.write_all(if write_old {
                    &old_contents
                } else {
                    &final_contents
                })?;
                write_old = !write_old;
            }
            file.rewind()?;
            file.write_all(&final_contents)?;
            file.sync_all()
        }
    });

    started.wait();
    let hashes = compute_content_hashes(path, ContentHashType::SHA256.into()).await?;
    writer.join().expect("writer thread panicked")?;

    assert_eq!(Some(expected), hashes.extra_hashes.sha256);
    Ok(())
}

#[tokio::test]
async fn hashing_a_continuously_changing_file_exhausts_retries() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("changing-file");
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)?
        .set_len(FILE_SIZE)?;

    let stop = Arc::new(AtomicBool::new(false));
    let started = Arc::new(Barrier::new(2));
    let writer = std::thread::spawn({
        let path = path.clone();
        let stop = stop.clone();
        let started = started.clone();
        move || -> std::io::Result<()> {
            let file = OpenOptions::new().write(true).open(path)?;
            let mut short = true;
            started.wait();
            while !stop.load(Ordering::Relaxed) {
                file.set_len(if short { FILE_SIZE / 2 } else { FILE_SIZE })?;
                short = !short;
            }
            Ok(())
        }
    });

    started.wait();
    let result = compute_content_hashes(path, ContentHashType::SHA256.into()).await;
    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer thread panicked")?;

    let error = result.expect_err("hashing should fail while the file keeps changing");
    assert!(
        error
            .to_string()
            .contains("file changed while hashing after 3 attempts"),
        "unexpected error: {error:#}"
    );
    Ok(())
}
