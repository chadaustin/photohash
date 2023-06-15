use crate::mpmc;
use crate::scan::IMPath;
use anyhow::Result;
use std::ffi::c_void;
use std::ffi::OsString;
use std::fs::Metadata;
use std::io;
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
use winapi::shared::ntstatus::STATUS_SUCCESS;
use winapi::um::winnt::FILE_ATTRIBUTE_NORMAL;
use winapi::um::winnt::FILE_LIST_DIRECTORY;
use winapi::um::winnt::FILE_SHARE_DELETE;
use winapi::um::winnt::FILE_SHARE_READ;
use winapi::um::winnt::FILE_SHARE_WRITE;
use winapi::um::winnt::FILE_TRAVERSE;
use winapi::um::winnt::SYNCHRONIZE;

pub struct DirectoryHandle {
    handle: HANDLE,
}

impl DirectoryHandle {}

fn to_device_path<P: AsRef<Path>>(path: P) -> io::Result<Vec<u16>> {
    let path = path.as_ref().canonicalize()?;
    eprintln!("path = {}", path.display());
    let mut v: Vec<u16> = path.as_os_str().encode_wide().collect();
    if !v.starts_with(&[b'\\', b'\\', b'?', b'\\'].map(|c| c as u16)) {
        // TODO: use InvalidFilename when stabilized?
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    // NtCreateFile requires absolute device paths.
    // \??\ is an alias for \DosDevices\ that's faster to parse in
    // Object Manager.
    v[1] = b'?' as u16;
    Ok(v)
}

pub fn windows_scan(paths: Vec<PathBuf>) -> mpmc::Receiver<(IMPath, Result<Metadata>)> {
    let (meta_tx, meta_rx) = mpmc::unbounded();

    for path in paths {
        let mut path = to_device_path(path).unwrap();

        let mut unicode_string = UNICODE_STRING {
            Length: 2 * path.len() as u16,
            MaximumLength: 2 * path.len() as u16,
            Buffer: path.as_mut_ptr(),
        };
        //() = unsafe { RtlInitUnicodeString(unicode_string.as_mut_ptr(), path.as_ptr()) };

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

        let mut io_status_block = MaybeUninit::zeroed();
        let mut handle = MaybeUninit::zeroed();

        let status: NTSTATUS = unsafe {
            NtCreateFile(
                handle.as_mut_ptr(),
                FILE_LIST_DIRECTORY | FILE_TRAVERSE | SYNCHRONIZE, // FILE_READ_ATTRIBUTES?
                object_attributes.as_mut_ptr(),
                io_status_block.as_mut_ptr(),
                ptr::null_mut(),       // AllocationSize
                FILE_ATTRIBUTE_NORMAL, // FileAttributes
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                FILE_OPEN,
                FILE_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_ALERT,
                ptr::null_mut(),
                0,
            )
        };

        if status == STATUS_SUCCESS {
            eprintln!("success!");
        } else {
            eprintln!("failure! {:#x}, {}", status as u32, unsafe {
                io_status_block.assume_init().Information
            });
            panic!();
        }

        let mut io_status_block = MaybeUninit::zeroed();
        const BUFFER_SIZE: u32 = 256 * 1024;
        let mut buffer: MaybeUninit<[u8; BUFFER_SIZE as usize]> = MaybeUninit::uninit();

        let status: NTSTATUS = unsafe {
            NtQueryDirectoryFile(
                handle.assume_init(),
                ptr::null_mut(), // Event
                None,            // ApcRoutine
                ptr::null_mut(), // ApcContext
                io_status_block.as_mut_ptr(),
                buffer.as_mut_ptr() as *mut c_void,
                BUFFER_SIZE,
                FileDirectoryInformation,
                FALSE,           // ReturnSingleEntry
                ptr::null_mut(), // FileName
                FALSE,           // RestartScan
            )
        };
        if status == STATUS_SUCCESS {
            eprintln!("success! {}", unsafe {
                io_status_block.assume_init().Information
            });
        } else {
            eprintln!("failure! {:#x}, {}", status as u32, unsafe {
                io_status_block.assume_init().Information
            });
            panic!();
        }

        let mut p = buffer.as_ptr() as *const FILE_DIRECTORY_INFORMATION;
        eprintln!("first p = {:?}", p);
        loop {
            let ref e = unsafe { p.as_ref().unwrap() };
            //eprintln!("p = {:?}", p);
            //eprintln!("NextEntryOffset: {}", e.NextEntryOffset);
            //eprintln!("FileNameLength: {}", e.FileNameLength);
            let name = OsString::from_wide(unsafe {
                std::slice::from_raw_parts(
                    ptr::addr_of!(e.FileName) as *const u16,
                    (e.FileNameLength / 2) as usize,
                )
            });
            eprintln!("{:?}", name);

            if e.NextEntryOffset == 0 {
                break;
            }
            p = unsafe {
                ((p as *const u8).offset(e.NextEntryOffset as isize))
                    as *const FILE_DIRECTORY_INFORMATION
            };
        }

        unsafe { NtClose(handle.assume_init()) };
    }

    _ = meta_tx;
    meta_rx
    //panic!("no return");
}
