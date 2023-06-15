use crate::mpmc;
use crate::scan::IMPath;
use anyhow::Result;
use std::ffi::c_void;
use std::ffi::OsString;
use std::fs::Metadata;
use std::io;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::os::windows::ffi::OsStringExt;
use std::path::Path;
use std::path::PathBuf;
use std::ptr;

use ntapi::ntioapi::FileDirectoryInformation;
use ntapi::ntioapi::NtCreateFile;
use ntapi::ntioapi::NtQueryDirectoryFile;
use ntapi::ntioapi::FILE_DIRECTORY_FILE;
use ntapi::ntioapi::FILE_DIRECTORY_INFORMATION;
use ntapi::ntioapi::FILE_OPEN;
use ntapi::ntioapi::FILE_SYNCHRONOUS_IO_ALERT;
use ntapi::ntioapi::FILE_SYNCHRONOUS_IO_NONALERT;
use ntapi::ntobapi::NtClose;
use ntapi::ntrtl::RtlInitUnicodeString;
use std::os::windows::ffi::OsStrExt;
use winapi::shared::ntdef::InitializeObjectAttributes;
use winapi::shared::ntdef::FALSE;
use winapi::shared::ntdef::HANDLE;
use winapi::shared::ntdef::NTSTATUS;
use winapi::shared::ntdef::OBJECT_ATTRIBUTES;
use winapi::shared::ntdef::OBJ_CASE_INSENSITIVE;
use winapi::shared::ntdef::OBJ_OPENIF;
use winapi::shared::ntdef::POBJECT_ATTRIBUTES;
use winapi::shared::ntdef::PUNICODE_STRING;
use winapi::shared::ntdef::PVOID;
use winapi::shared::ntdef::UNICODE_STRING;
use winapi::shared::ntstatus::*;
use winapi::um::heapapi::GetProcessHeap;
use winapi::um::heapapi::HeapAlloc;
use winapi::um::heapapi::HeapFree;
use winapi::um::winnt::FILE_ATTRIBUTE_NORMAL;
use winapi::um::winnt::FILE_LIST_DIRECTORY;
use winapi::um::winnt::FILE_SHARE_DELETE;
use winapi::um::winnt::FILE_SHARE_READ;
use winapi::um::winnt::FILE_SHARE_WRITE;
use winapi::um::winnt::FILE_TRAVERSE;
use winapi::um::winnt::HEAP_GENERATE_EXCEPTIONS;
use winapi::um::winnt::SYNCHRONIZE;

use static_assertions::const_assert;

// Ensure the buffer can always hold one max-length component.
// Though prefer larger buffers for fewer syscalls.
const BUFFER_SIZE: usize = 65536;
const_assert!(BUFFER_SIZE >= std::mem::size_of::<FILE_DIRECTORY_INFORMATION>() + 255 * 2);

unsafe fn walloc(size: usize) -> *mut u8 {
    HeapAlloc(GetProcessHeap(), HEAP_GENERATE_EXCEPTIONS, size) as *mut u8
}

unsafe fn wfree(p: *mut u8) {
    _ = HeapFree(GetProcessHeap(), 0, p as PVOID);
}

pub struct DirectoryHandle {
    handle: HANDLE,
}

impl Drop for DirectoryHandle {
    fn drop(&mut self) {
        // TODO: Worth asserting if return value isn't STATUS_SUCCESS?
        let result = unsafe { NtClose(self.handle) };
    }
}

impl DirectoryHandle {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<DirectoryHandle> {
        let mut path = to_device_path(path)?;

        let mut unicode_string = unicode_string(&path)?;

        let mut object_attributes = MaybeUninit::uninit();
        () = unsafe {
            InitializeObjectAttributes(
                object_attributes.as_mut_ptr(),
                &mut unicode_string,
                OBJ_CASE_INSENSITIVE | OBJ_OPENIF,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };

        Self::do_open(object_attributes.as_mut_ptr())
    }

    pub fn open_child(&self, path: &[u16]) -> io::Result<DirectoryHandle> {
        let mut unicode_string = unicode_string(path)?;

        let mut object_attributes = MaybeUninit::uninit();
        () = unsafe {
            InitializeObjectAttributes(
                object_attributes.as_mut_ptr(),
                &mut unicode_string,
                OBJ_CASE_INSENSITIVE | OBJ_OPENIF,
                self.handle,
                ptr::null_mut(),
            )
        };

        Self::do_open(object_attributes.as_mut_ptr())
    }

    pub fn entries(&self) -> Entries<'_> {
        Entries {
            handle: self.handle,
            done: false,
            buffer: unsafe { walloc(BUFFER_SIZE) },
            next: None,
            marker: PhantomData,
        }
    }

    fn do_open(object_attributes: *mut OBJECT_ATTRIBUTES) -> io::Result<DirectoryHandle> {
        let mut io_status_block = MaybeUninit::zeroed();
        let mut handle = MaybeUninit::zeroed();
        let status: NTSTATUS = unsafe {
            NtCreateFile(
                handle.as_mut_ptr(),
                FILE_LIST_DIRECTORY | FILE_TRAVERSE | SYNCHRONIZE, // FILE_READ_ATTRIBUTES?
                object_attributes,
                io_status_block.as_mut_ptr(),
                ptr::null_mut(),       // AllocationSize
                FILE_ATTRIBUTE_NORMAL, // FileAttributes
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                FILE_OPEN,
                // TODO: Compare FILE_SYNCHRONOUS_IO_ALERT to FILE_SYNCHRONOUS_IO_NONALERT.
                FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT,
                ptr::null_mut(),
                0,
            )
        };

        if status != STATUS_SUCCESS {
            // TODO: use io_status_block.assume_init().Information?
            return Err(to_error_kind(status).into());
        }

        Ok(DirectoryHandle {
            handle: unsafe { handle.assume_init() },
        })
    }
}

pub struct Entry<'a> {
    name: &'a [u16],
    btime: u64,
    atime: u64,
    mtime: u64,
    ctime: u64,
    size: u64,
    attr: u32,
}

impl<'a> Entry<'a> {
    fn os_str(&self) -> OsString {
        OsString::from_wide(&self.name)
    }
}

pub struct Entries<'a> {
    handle: HANDLE,
    done: bool,
    buffer: *mut u8,
    next: Option<u32>,
    marker: PhantomData<&'a ()>,
}

impl Drop for Entries<'_> {
    fn drop(&mut self) {
        unsafe { wfree(self.buffer) }
    }
}

impl Entries<'_> {
    fn next(&mut self) -> Option<io::Result<Entry<'_>>> {
        if self.done {
            return None;
        }

        let next = match self.next {
            Some(next) => next,
            None => {
                eprintln!("calling");
                let mut io_status_block = MaybeUninit::zeroed();
                let status: NTSTATUS = unsafe {
                    NtQueryDirectoryFile(
                        self.handle,
                        ptr::null_mut(), // Event
                        None,            // ApcRoutine
                        ptr::null_mut(), // ApcContext
                        io_status_block.as_mut_ptr(),
                        self.buffer as *mut c_void,
                        BUFFER_SIZE as u32,
                        FileDirectoryInformation,
                        FALSE,           // ReturnSingleEntry
                        ptr::null_mut(), // FileName
                        FALSE,           // RestartScan
                    )
                };
                match status {
                    STATUS_SUCCESS => 0,
                    STATUS_NO_MORE_FILES => {
                        self.done = true;
                        return None;
                    }
                    _ => {
                        self.done = true;
                        // TODO: use io_status_block.assume_init().Information?
                        return Some(Err(to_error_kind(status).into()));
                    }
                }
            }
        };

        let e: &FILE_DIRECTORY_INFORMATION = unsafe {
            let p = self.buffer.offset(next as isize) as *const FILE_DIRECTORY_INFORMATION;
            p.as_ref().unwrap()
        };
        self.next = if e.NextEntryOffset == 0 {
            None
        } else {
            Some(next + e.NextEntryOffset)
        };
        Some(Ok(unsafe {
            Entry {
                name: std::slice::from_raw_parts(
                    ptr::addr_of!(e.FileName) as *const u16,
                    (e.FileNameLength / 2) as usize,
                ),
                // TODO: Explore what it means for any of these to
                // have the sign bit set.
                btime: *e.CreationTime.QuadPart() as u64,
                atime: *e.LastAccessTime.QuadPart() as u64,
                mtime: *e.LastWriteTime.QuadPart() as u64,
                ctime: *e.ChangeTime.QuadPart() as u64,
                size: *e.EndOfFile.QuadPart() as u64,
                attr: e.FileAttributes,
            }
        }))
    }
}

// ErrorKind is not very expressive. It will get slightly better with
// https://github.com/rust-lang/rust/issues/86442 In the meantime, map
// onto equivalents when possible. We could support a new Error type
// with a lossy conversion into io::Error.
fn to_error_kind(status: NTSTATUS) -> ErrorKind {
    match status {
        STATUS_SUCCESS => panic!("success"),
        STATUS_ALERTED => ErrorKind::Interrupted,
        STATUS_TIMEOUT => ErrorKind::TimedOut,
        STATUS_PENDING => ErrorKind::WouldBlock,
        //STATUS_REPARSE => ErrorKind::FilesystemLoop, // ??
        //STATUS_MORE_ENTRIES => ;
        //STATUS_NOT_ALL_ASSIGNED => ;
        // ...
        //STATUS_REPARSE_OBJECT => ErrorKind::FilesystemLoop, // ??
        STATUS_INVALID_INFO_CLASS => ErrorKind::InvalidInput,
        STATUS_INVALID_HANDLE => ErrorKind::InvalidInput,
        STATUS_INVALID_PARAMETER => ErrorKind::InvalidInput,
        // ...
        // more?
        // TODO:
        _ => ErrorKind::Other,
    }
}

fn to_device_path<P: AsRef<Path>>(path: P) -> io::Result<Vec<u16>> {
    let path = path.as_ref().canonicalize()?;
    let mut v: Vec<u16> = path.as_os_str().encode_wide().collect();
    if !v.starts_with(&[b'\\', b'\\', b'?', b'\\'].map(|c| c as u16)) {
        // TODO: use InvalidFilename when stabilized?
        return Err(ErrorKind::InvalidInput.into());
    }
    // NtCreateFile requires absolute device paths.
    // \??\ is an alias for \DosDevices\ that's faster to parse in
    // Object Manager.
    v[1] = b'?' as u16;
    Ok(v)
}

fn unicode_string(path: &[u16]) -> io::Result<UNICODE_STRING> {
    // The documentation
    if path.len() >= 32767 {
        // TODO: use InvalidFilename when stabilized?
        return Err(ErrorKind::InvalidInput.into());
    }

    // We could use RtlInitUnicodeString but that's a function
    // call and these fields have obvious meaning.
    Ok(UNICODE_STRING {
        Length: 2 * path.len() as u16,
        MaximumLength: 2 * path.len() as u16,
        Buffer: path.as_ptr() as *mut u16,
    })
}

pub fn windows_scan(paths: Vec<PathBuf>) -> Result<mpmc::Receiver<(IMPath, Result<Metadata>)>> {
    let (meta_tx, meta_rx) = mpmc::unbounded();

    for path in paths {
        let handle = DirectoryHandle::open(path)?;

        let mut entries = handle.entries();
        while let Some(info) = entries.next() {
            let info = info?;
            eprintln!("name: {}", info.os_str().to_string_lossy());
        }
    }

    _ = meta_tx;
    Ok(meta_rx)
}
