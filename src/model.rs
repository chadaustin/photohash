use std::borrow::Borrow;
use std::fs::Metadata;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::time::SystemTime;

pub type Hash<const N: usize> = [u8; N];
pub type Hash20 = [u8; 20];
pub type Hash32 = [u8; 32];

/// Only support unicode filenames for efficient conversion into and
/// out of SQLite.
pub type IMPath = String;

/// Platform-independent subset of file information to be stored in
/// SQLite.
#[derive(Debug, PartialEq)]
pub struct FileInfo {
    /// 0 on Windows for now. May contain file_index() when the API is
    /// stabilized.
    pub inode: u64,
    pub size: u64,
    /// mtime in the local platform's units
    pub mtime: SystemTime,
}

impl<T: Borrow<Metadata>> From<T> for FileInfo {
    fn from(metadata: T) -> FileInfo {
        let metadata = metadata.borrow();
        FileInfo {
            inode: metadata.ino(),
            size: metadata.len(),
            mtime: metadata
                .modified()
                .expect("requires a platform that supports mtime"),
        }
    }
}

#[cfg(not(unix))]
trait FakeInode {
    fn ino(&self) -> u64;
}

#[cfg(not(unix))]
impl FakeInode for std::fs::Metadata {
    fn ino(&self) -> u64 {
        0
    }
}

#[derive(Debug, PartialEq)]
pub struct ContentMetadata {
    /// Platform independent subset of `std::fs::Metadata` used to
    /// decide whether to recompute blake3.
    pub file_info: FileInfo,

    /// Primary content hash used to deduplicate image metadata.
    pub blake3: Hash32,
}

/// Secondary image metadata used for fast duplicate detection,
/// including rotation-independent and perceptual hashes.
#[derive(Debug, PartialEq)]
pub struct ImageMetadata {
    /// If we're hashing the pixels, we might as well store the
    /// dimensions. These are the parsed pixel dimensions independent
    /// of EXIF.
    pub dimensions: Option<(u32, u32)>,

    /// 256-bit blake3 hash of rotation-independent pixel data.
    pub jpegrothash: Option<Hash32>,

    /// 256-bit blockhash as computed by the `blockhash` crate.
    /// image_hasher (img_hash) also provides a blockhash
    /// implementation, but it's very slow.
    pub blockhash256: Option<Hash32>,
}

impl ImageMetadata {
    pub fn invalid() -> Self {
        Self {
            dimensions: None,
            jpegrothash: None,
            blockhash256: None,
        }
    }

    pub fn is_invalid(&self) -> bool {
        self.dimensions.is_none()
    }
}
