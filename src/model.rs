use std::path::PathBuf;
use std::time::SystemTime;

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
    pub blockhash: Hash32,
}
