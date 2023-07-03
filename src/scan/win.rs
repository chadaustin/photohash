use crate::model::FileInfo;
use crate::model::IMPath;
use crate::mpmc;
use anyhow::Result;
use ntapi::ntioapi::FileDirectoryInformation;
use ntapi::ntioapi::NtCreateFile;
use ntapi::ntioapi::NtQueryDirectoryFile;
use ntapi::ntioapi::FILE_DIRECTORY_FILE;
use ntapi::ntioapi::FILE_DIRECTORY_INFORMATION;
use ntapi::ntioapi::FILE_OPEN;
//use ntapi::ntioapi::FILE_SYNCHRONOUS_IO_ALERT;
use ntapi::ntioapi::FILE_SYNCHRONOUS_IO_NONALERT;
use ntapi::ntobapi::NtClose;
use static_assertions::const_assert;
use std::collections::VecDeque;
use std::ffi::c_void;
use std::ffi::OsString;
use std::io;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::ffi::OsStringExt;
use std::path::Path;
use std::ptr;
use std::sync::mpsc::SendError;
use std::sync::mpsc::SyncSender;
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use winapi::shared::ntdef::InitializeObjectAttributes;
use winapi::shared::ntdef::FALSE;
use winapi::shared::ntdef::HANDLE;
use winapi::shared::ntdef::NTSTATUS;
use winapi::shared::ntdef::OBJECT_ATTRIBUTES;
use winapi::shared::ntdef::OBJ_CASE_INSENSITIVE;
use winapi::shared::ntdef::OBJ_OPENIF;
use winapi::shared::ntdef::PVOID;
use winapi::shared::ntdef::UNICODE_STRING;
use winapi::shared::ntstatus::*;
use winapi::um::heapapi::GetProcessHeap;
use winapi::um::heapapi::HeapAlloc;
use winapi::um::heapapi::HeapFree;
use winapi::um::winnt::FILE_ATTRIBUTE_DIRECTORY;
use winapi::um::winnt::FILE_ATTRIBUTE_NORMAL;
use winapi::um::winnt::FILE_LIST_DIRECTORY;
use winapi::um::winnt::FILE_SHARE_DELETE;
use winapi::um::winnt::FILE_SHARE_READ;
use winapi::um::winnt::FILE_SHARE_WRITE;
use winapi::um::winnt::FILE_TRAVERSE;
use winapi::um::winnt::HEAP_GENERATE_EXCEPTIONS;
use winapi::um::winnt::SYNCHRONIZE;

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

struct HandleWrapper(HANDLE);
unsafe impl Send for HandleWrapper {}

static CLOSE_HANDLE: OnceLock<SyncSender<HandleWrapper>> = OnceLock::new();

fn sync_close_handle(handle: HANDLE) {
    // TODO: worth asserting if the return value isn't STATUS_SUCCESS?
    _ = unsafe { NtClose(handle) };
}

// In my benchmarks against an SMB share, 10-15% of the scan time
// is spent in NtClose. Push that into another thread.
fn async_close_handle(handle: HANDLE) {
    let tx = CLOSE_HANDLE.get_or_init(|| {
        let (close_tx, close_rx) = std::sync::mpsc::sync_channel(1000);

        thread::Builder::new()
            .name("closehandle".to_string())
            .spawn(move || {
                while let Ok(HandleWrapper(handle)) = close_rx.recv() {
                    sync_close_handle(handle);
                }
            })
            .unwrap();

        close_tx
    });

    match tx.send(HandleWrapper(handle)) {
        Ok(()) => (),
        Err(SendError(h)) => sync_close_handle(h.0),
    }
}

pub struct DirectoryHandle {
    handle: HANDLE,
}

impl Drop for DirectoryHandle {
    fn drop(&mut self) {
        async_close_handle(self.handle)
    }
}

impl DirectoryHandle {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<DirectoryHandle> {
        let path = to_device_path(path)?;

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

    fn is_dir(&self) -> bool {
        self.attr & FILE_ATTRIBUTE_DIRECTORY == FILE_ATTRIBUTE_DIRECTORY
    }

    fn mtime(&self) -> SystemTime {
        let windows_epoch = UNIX_EPOCH - Duration::from_secs(11644473600);
        // TODO: handle negative mtime
        // TODO: handle overflow
        windows_epoch + Duration::from_nanos(self.mtime * 100)
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

pub fn windows_scan(paths: &[&Path]) -> Result<mpmc::Receiver<(IMPath, Result<FileInfo>)>> {
    let paths = super::canonicalize_all(paths)?;

    let (meta_tx, meta_rx) = mpmc::unbounded();

    thread::Builder::new()
        .name("winscan".to_string())
        .spawn(move || {
            // Depth first to minimize the maximum size of this queue
            // to O(depth * single_dir_entries);
            let mut queue = VecDeque::with_capacity(paths.len());

            for path in paths {
                queue.push_back((None, path));
            }

            while let Some((handle, path)) = queue.pop_front() {
                let handle = handle.unwrap_or_else(|| {
                    // TODO: propagate error
                    DirectoryHandle::open(&path).unwrap()
                });

                // TODO: double-buffer this allocation
                let mut dirs_to_add = Vec::new();

                let mut entries = handle.entries();
                while let Some(e) = entries.next() {
                    let e = e.unwrap();
                    if e.name == [b'.' as u16] || e.name == [b'.' as u16, b'.' as u16] {
                        continue;
                    }

                    // Construct a full child path. I hate that this
                    // has two allocations. I haven't found a way to
                    // bypass that.
                    let child_full_path = Path::join(&path, OsString::from_wide(e.name));
                    //eprintln!("child_full_path {}", child_full_path.display());

                    if e.is_dir() {
                        // TODO: handle errors
                        let child = handle.open_child(e.name).unwrap();
                        dirs_to_add.push((Some(child), child_full_path));
                    } else {
                        if let Some(utf8_full_path) = child_full_path.to_str() {
                            // TODO: return on Err?
                            _ = meta_tx.send((
                                utf8_full_path.to_string(),
                                Ok(FileInfo {
                                    // TODO: We do technically have
                                    // the inode number if we call
                                    // NtQueryDirectoryFileEx with
                                    // FileIdBothDirectoryInformation.
                                    inode: 0,
                                    size: e.size,
                                    mtime: e.mtime(),
                                }),
                            ));
                        }
                    }
                }

                while let Some(to_add) = dirs_to_add.pop() {
                    queue.push_front(to_add);
                }
            }
        })
        .unwrap();

    Ok(meta_rx)
}
