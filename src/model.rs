use std::fs::Metadata;
use std::path::PathBuf;
use std::time::SystemTime;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

pub type Hash20 = [u8; 20];
pub type Hash32 = [u8; 32];

/// Platform-independent subset of file information to be stored in SQLite.
#[derive(PartialEq)]
pub struct FileInfo {
    /// 0 on Windows for now. May contain file_index() when the API is stabilized.
    pub inode: u64,
    pub size: u64,
    /// mtime in the local platform's units
    pub mtime: SystemTime,
}

impl From<&Metadata> for FileInfo {
    fn from(metadata: &Metadata) -> FileInfo {
        FileInfo {
            inode: metadata.ino(),
            size: metadata.len(),
            mtime: metadata
                .modified()
                .expect("requires a platform that supports mtime"),
        }
    }
}

pub struct ContentMetadata {
    pub path: PathBuf,

    pub file_info: FileInfo,

    // Content attributes
    // MD5? SHA-1?
    pub blake3: Hash32,
}

pub struct ImageMetadata {
    pub image_width: u32,
    pub image_height: u32,
    pub blockhash256: Hash32,
}
