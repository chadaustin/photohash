use std::borrow::Borrow;
use std::fs::Metadata;
use std::time::SystemTime;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

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
    pub file_info: FileInfo,

    // Content attributes
    // MD5? SHA-1?
    pub blake3: Hash32,
}

#[derive(Debug, PartialEq)]
pub struct ImageMetadata {
    pub image_width: u32,
    pub image_height: u32,
    pub blockhash256: Hash32,
}
