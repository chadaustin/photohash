use crate::iopool;
use crate::model::ExtraHashes;
use crate::model::FileInfo;
use crate::model::Hash32;
use crate::model::ImageMetadata;
use arrayvec::ArrayVec;
use digest::DynDigest;
use enumset::EnumSetType;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

mod heic;
mod jpeg;

const READ_SIZE: usize = 65536;

const USE_IMAGE_HASHER_BLOCKHASH: bool = false;

const JPEG_EXTENSIONS: &[&str] = &["jpg", "jpeg", "jpe", "jif", "jfif", "jfi"];
const HEIC_EXTENSIONS: &[&str] = &["heif", "heic"];

// TODO: We could replace all of the custom impls with thiserror if we
// had a PathBuf wrapper type that Displayed with dunce::simplified.
#[derive(Debug)]
pub enum ImageMetadataError {
    //#[error("not a supported photo file: {path}")]
    UnsupportedPhoto {
        path: PathBuf,
        source: Option<anyhow::Error>,
    },
    //#[error("unexpected error reading image metadata")]
    Other(/*#[from]*/ anyhow::Error),
}

impl std::error::Error for ImageMetadataError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::UnsupportedPhoto { path, source } => {
                _ = path;
                source.as_ref().map(|e| e.as_ref())
            }
            Self::Other(source) => Some(source.as_ref()),
        }
    }
}

impl std::fmt::Display for ImageMetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedPhoto { path, source } => {
                _ = source;
                // All of these custom instances are just to customize how path is displayed.
                write!(
                    f,
                    "not a supported photo file: {}",
                    dunce::simplified(path).display()
                )
            }
            Self::Other(source) => {
                _ = source;
                write!(f, "unexpected error reading image metadata")
            }
        }
    }
}

impl From<anyhow::Error> for ImageMetadataError {
    fn from(e: anyhow::Error) -> Self {
        Self::Other(e)
    }
}

// TODO: How can I avoid hand-writing these instances?
// std::error::Error overlaps with From<T> for T.

impl From<turbojpeg::Error> for ImageMetadataError {
    fn from(e: turbojpeg::Error) -> Self {
        Self::Other(e.into())
    }
}

impl From<std::array::TryFromSliceError> for ImageMetadataError {
    fn from(e: std::array::TryFromSliceError) -> Self {
        Self::Other(e.into())
    }
}

impl From<std::io::Error> for ImageMetadataError {
    fn from(e: std::io::Error) -> Self {
        Self::Other(e.into())
    }
}

impl From<tokio::task::JoinError> for ImageMetadataError {
    fn from(e: tokio::task::JoinError) -> Self {
        Self::Other(e.into())
    }
}

fn unsupported_photo<E: Into<anyhow::Error>>(
    path: &Path,
) -> impl FnOnce(E) -> ImageMetadataError + '_ {
    move |e| ImageMetadataError::UnsupportedPhoto {
        path: path.to_owned(),
        source: Some(e.into()),
    }
}

fn has_any_extension(path: &Path, exts: &[&str]) -> bool {
    path.extension()
        .map(|e| exts.iter().any(|x| e.eq_ignore_ascii_case(x)))
        .unwrap_or(false)
}

/// Returns true if path has a common JPEG file extension.
pub fn is_jpeg<P: AsRef<Path>>(path: P) -> bool {
    has_any_extension(path.as_ref(), JPEG_EXTENSIONS)
}

/// Returns true if path has a common HEIC file extension.
pub fn is_heic<P: AsRef<Path>>(path: P) -> bool {
    has_any_extension(path.as_ref(), HEIC_EXTENSIONS)
}

/// Returns true if path represents a file that may have ImageMetadata.
pub fn may_have_metadata<P: AsRef<Path>>(path: P, file_info: &FileInfo) -> bool {
    // Skip AppleDouble files.
    if path
        .as_ref()
        .file_name()
        .and_then(|b| b.to_str())
        .unwrap_or("")
        .starts_with("._")
    {
        return false;
    }
    // Too large to decode.
    if file_info.size > 40_000_000 {
        return false;
    }
    is_jpeg(&path) || is_heic(&path)
}

/// Computes and returns image metadata including dimensions and
/// perceptual hashes.
pub async fn compute_image_hashes<P: AsRef<Path>>(
    path: P,
) -> Result<ImageMetadata, ImageMetadataError> {
    if is_jpeg(&path) {
        Ok(jpeg::compute_image_hashes(path.as_ref()).await?)
    } else if is_heic(&path) {
        Ok(heic::compute_image_hashes(path.as_ref()).await?)
    } else {
        Err(ImageMetadataError::UnsupportedPhoto {
            path: path.as_ref().to_owned(),
            source: None,
        })
    }
}

#[derive(Debug, EnumSetType)]
pub enum ContentHashType {
    BLAKE3,
    MD5,
    SHA1,
    SHA256,
}

pub type ContentHashSet = enumset::EnumSet<ContentHashType>;

pub fn get_hasher(hash: ContentHashType) -> Box<dyn DynDigest> {
    match hash {
        ContentHashType::BLAKE3 => Box::new(blake3::Hasher::new()),
        ContentHashType::MD5 => Box::new(md5::Md5::default()),
        ContentHashType::SHA1 => Box::new(sha1::Sha1::default()),
        ContentHashType::SHA256 => Box::new(sha2::Sha256::default()),
    }
}

enum WorkerPool {
    Tokio,
    IO,
}

fn select_worker_pool(hashes: ContentHashSet) -> WorkerPool {
    // My NAS, an Atom D2700DC with spinning iron disks, can sustain
    // about 100 MB/s of read IO. `photohash bench hashes` gives:
    //
    // BLAKE3: 421.77 MB/s
    // MD5: 243.54 MB/s
    // SHA1: 124.63 MB/s
    // SHA256: 56.17 MB/s
    // -----
    // total: 30.96 MB/s
    //
    // Therefore, computing extra hashes is CPU-bound. If SHA256 is in
    // the set, we can simply reuse the Tokio pool.
    if hashes.contains(ContentHashType::SHA256) {
        WorkerPool::Tokio
    } else {
        WorkerPool::IO
    }
}

#[derive(Debug, Default)]
pub struct ContentHashes {
    pub blake3: Option<Hash32>,
    pub extra_hashes: ExtraHashes,
}

struct Hasher<'a> {
    digest: &'a mut (dyn DynDigest + Send),
    target: &'a mut [u8],
}

impl Hasher<'_> {
    fn update(&mut self, data: &[u8]) {
        self.digest.update(data)
    }
    fn finalize(&mut self) {
        self.digest
            .finalize_into_reset(self.target)
            .expect("incorrect hash length");
    }
}

pub async fn compute_content_hashes(
    path: PathBuf,
    which: ContentHashSet,
) -> anyhow::Result<ContentHashes> {
    let mut result = ContentHashes::default();
    apply_content_hashes(&mut result, path, which).await?;
    Ok(result)
}

async fn apply_content_hashes(
    result: &mut ContentHashes,
    path: PathBuf,
    which: ContentHashSet,
) -> anyhow::Result<()> {
    let mut blake3_hasher: Option<blake3::Hasher> = None;
    let mut md5_hasher: Option<md5::Md5> = None;
    let mut sha1_hasher: Option<sha1::Sha1> = None;
    let mut sha256_hasher: Option<sha2::Sha256> = None;

    // 4 = blake3 + md5 + sha1 + sha256
    const HASHER_COUNT: usize = ContentHashSet::variant_count() as usize;
    let mut hashers = ArrayVec::<_, HASHER_COUNT>::new();

    if which.contains(ContentHashType::BLAKE3) {
        hashers.push(Hasher {
            digest: blake3_hasher.get_or_insert_default(),
            target: result.blake3.get_or_insert_default(),
        });
    }

    if which.contains(ContentHashType::MD5) {
        hashers.push(Hasher {
            digest: md5_hasher.get_or_insert_default(),
            target: result.extra_hashes.md5.get_or_insert_default(),
        });
    }

    if which.contains(ContentHashType::SHA1) {
        hashers.push(Hasher {
            digest: sha1_hasher.get_or_insert_default(),
            target: result.extra_hashes.sha1.get_or_insert_default(),
        });
    }

    if which.contains(ContentHashType::SHA256) {
        hashers.push(Hasher {
            digest: sha256_hasher.get_or_insert_default(),
            target: result.extra_hashes.sha256.get_or_insert_default(),
        });
    }

    match select_worker_pool(which) {
        WorkerPool::Tokio => {
            let mut file = tokio::fs::File::open(path).await?;
            // TODO: 64KiB * (100 MB/s) = 655 microseconds, perhaps
            // below scheduling quanta. Consider a larger value.
            let mut buffer = [0u8; READ_SIZE];
            loop {
                let read = file.read(&mut buffer);
                let n = read.await?;
                if n == 0 {
                    break;
                }
                for h in &mut hashers {
                    h.update(&buffer[..n]);
                }
            }
            for mut h in hashers {
                h.finalize();
            }
            Ok(())
        }
        WorkerPool::IO => {
            iopool::run_in_io_pool_local(|| {
                let mut file = std::fs::File::open(path)?;
                let mut buffer = [0u8; READ_SIZE];
                loop {
                    let n = file.read(&mut buffer)?;
                    if n == 0 {
                        break;
                    }
                    for h in &mut hashers {
                        h.update(&buffer[..n]);
                    }
                }
                for mut h in hashers {
                    h.finalize();
                }
                Ok(())
            })
            .await
        }
    }
}

pub async fn update_content_hashes(
    current: &mut ContentHashes,
    path: PathBuf,
    desired: ContentHashSet,
) -> anyhow::Result<ContentHashSet> {
    let mut current_types = ContentHashSet::empty();
    if current.blake3.is_some() {
        current_types |= ContentHashType::BLAKE3;
    }
    if current.extra_hashes.md5.is_some() {
        current_types |= ContentHashType::MD5;
    }
    if current.extra_hashes.sha1.is_some() {
        current_types |= ContentHashType::SHA1;
    }
    if current.extra_hashes.sha256.is_some() {
        current_types |= ContentHashType::SHA256;
    }

    let to_compute = desired - current_types;
    if to_compute != ContentHashSet::empty() {
        () = apply_content_hashes(current, path, to_compute).await?;
    }

    Ok(to_compute)
}

pub async fn compute_blake3(path: PathBuf) -> anyhow::Result<Hash32> {
    Ok(compute_content_hashes(path, ContentHashType::BLAKE3.into())
        .await?
        .blake3
        .expect("blake3 requested but not returned"))
}

pub async fn compute_extra_hashes(path: PathBuf) -> anyhow::Result<ExtraHashes> {
    let hashes =
        compute_content_hashes(path, ContentHashSet::all() - ContentHashType::BLAKE3).await?;
    Ok(hashes.extra_hashes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_jpeg() {
        assert!(!is_jpeg("foo"));
        assert!(!is_jpeg("jpeg"));
        assert!(!is_jpeg("cake.heic"));
        assert!(!is_jpeg("readme.txt"));

        assert!(is_jpeg("foo.jpeg"));
        assert!(is_jpeg("foo.JPG"));
        assert!(is_jpeg("Hello World.Jpeg"));
    }
}
