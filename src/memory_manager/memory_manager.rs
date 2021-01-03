use memmap::{Mmap, MmapMut};
use std::fs::{File, OpenOptions};
use std::path::Path;

/// A read-only memory mapped file.
pub struct MmapReadOnlyFile {
    mmap: Mmap,
    len: u64,
}

impl MmapReadOnlyFile {
    pub fn from_file(file: &File) -> Self {
        let len = file.metadata().unwrap().len();
        let mmap = unsafe { Mmap::map(&file) }.unwrap();
        Self { mmap, len }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn read<T>(&self, pos: usize) -> *const T {
        unsafe { self.mmap.as_ptr().add(pos) as *const T }
    }
}

/// A memory mapped file.
pub struct MmapFile {
    file: File,
    mmap: MmapMut,
    len: u64,
}

impl MmapFile {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        Self::from_file(file)
    }

    pub fn from_file(file: File) -> Self {
        let len = file.metadata().unwrap().len();
        let mmap = if len == 0 {
            MmapMut::map_anon(1).unwrap()
        } else {
            unsafe { MmapMut::map_mut(&file).unwrap() }
        };
        Self { file, mmap, len }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn resize(&mut self, new_len: usize) {
        self.len = new_len as u64;
        self.mmap = MmapMut::map_anon(1).unwrap();
        self.file.set_len(self.len).unwrap();
        if new_len != 0 {
            self.mmap = unsafe { MmapMut::map_mut(&self.file).unwrap() }
        }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.mmap.as_ptr()
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.mmap.as_mut_ptr()
    }

    pub fn read<T>(&self, pos: usize) -> *const T {
        unsafe { self.mmap.as_ptr().add(pos) as *const T }
    }

    pub fn write<T>(&mut self, pos: usize, data: *const T, count: usize) {
        unsafe { std::ptr::copy(data, self.mmap.as_mut_ptr().add(pos) as *mut T, count) }
    }
}

/// A memory manager to hide the underlying type of the memory buffer.
pub enum MemoryManager {
    /// A memory buffer.
    Mem(Vec<u8>),
    /// A read-only memory mapped buffer.
    MmapReadOnly(MmapReadOnlyFile),
    /// A memory mapped buffer.
    Mmap(MmapFile),
    /// A sink.
    Sink,
}

impl MemoryManager {
    pub fn len(&self) -> usize {
        match self {
            MemoryManager::Mem(vec) => vec.len(),
            MemoryManager::MmapReadOnly(mmapfile) => mmapfile.len(),
            MemoryManager::Mmap(mmapfile) => mmapfile.len(),
            MemoryManager::Sink => 0,
        }
    }

    pub fn resize(&mut self, new_len: usize) {
        match self {
            MemoryManager::Mem(vec) => vec.resize(new_len, 0),
            MemoryManager::MmapReadOnly(_) => panic!("Cannot resize read only file"),
            MemoryManager::Mmap(mmapfile) => mmapfile.resize(new_len),
            MemoryManager::Sink => (),
        }
    }

    pub fn read<T>(&self, pos: usize) -> *const T {
        match self {
            MemoryManager::Mem(vec) => unsafe { vec.as_ptr().add(pos) as *const T },
            MemoryManager::MmapReadOnly(mmapfile) => mmapfile.read(pos),
            MemoryManager::Mmap(mmapfile) => mmapfile.read(pos),
            MemoryManager::Sink => std::ptr::null(),
        }
    }

    pub fn read_slice<T>(&self, pos: usize, count: usize) -> &[T] {
        match self {
            MemoryManager::Mem(vec) => unsafe {
                std::slice::from_raw_parts(vec.as_ptr().add(pos) as *const T, count)
            },
            MemoryManager::MmapReadOnly(mmapfile) => unsafe {
                std::slice::from_raw_parts(mmapfile.read(pos), count)
            },
            MemoryManager::Mmap(mmapfile) => unsafe {
                std::slice::from_raw_parts(mmapfile.read(pos), count)
            },
            MemoryManager::Sink => &[],
        }
    }

    pub fn write<T>(&mut self, pos: usize, data: *const T, count: usize) {
        match self {
            MemoryManager::Mem(vec) => unsafe {
                std::ptr::copy(data, vec.as_mut_ptr().add(pos) as *mut T, count)
            },
            MemoryManager::MmapReadOnly(_) => panic!("Cannot write read-only file"),
            MemoryManager::Mmap(mmapfile) => mmapfile.write(pos, data, count),
            MemoryManager::Sink => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Write, mem::size_of};

    #[test]
    fn test_mem_len() {
        let mm = MemoryManager::Mem(vec![1, 2, 3, 4, 5]);
        assert_eq!(mm.len(), 5);
    }

    #[test]
    fn test_mem_read() {
        let mm = MemoryManager::Mem(vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(mm.read_slice::<u8>(0, mm.len()), [1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_mem_write() {
        let mut mm = MemoryManager::Mem(vec![1, 2, 3, 4, 5, 6]);
        mm.write(0, &10u8 as *const u8, 1);
        assert_eq!(mm.read_slice::<u8>(0, mm.len()), [10, 2, 3, 4, 5, 6]);
        mm.write(1, &20u8 as *const u8, 1);
        assert_eq!(mm.read_slice::<u8>(0, mm.len()), [10, 20, 3, 4, 5, 6]);
    }

    #[test]
    fn test_mem_shrink_expand() {
        let mut mm = MemoryManager::Mem(vec![1, 2, 3, 4, 5, 6]);
        mm.resize(3);
        assert_eq!(mm.read_slice::<u8>(0, mm.len()), [1, 2, 3]);
        mm.resize(6);
        assert_eq!(mm.read_slice::<u8>(0, mm.len()), [1, 2, 3, 0, 0, 0]);
        mm.resize(0);
        assert_eq!(mm.read_slice::<u8>(0, mm.len()), []);
        mm.resize(3);
        assert_eq!(mm.read_slice::<u8>(0, mm.len()), [0, 0, 0]);
    }

    fn new_mmap_mm() -> MemoryManager {
        let mut file = tempfile::tempfile().unwrap();
        file.write(&[1, 2, 3, 4, 5, 6]).unwrap();
        let mmapfile = MmapFile::from_file(file);
        MemoryManager::Mmap(mmapfile)
    }

    fn new_empty_mmap_mm() -> MemoryManager {
        let file = tempfile::tempfile().unwrap();
        let mmapfile = MmapFile::from_file(file);
        MemoryManager::Mmap(mmapfile)
    }

    #[test]
    fn test_mmap_len() {
        let mm = new_mmap_mm();
        assert_eq!(mm.len(), 6);
        let mm = new_empty_mmap_mm();
        assert_eq!(mm.len(), 0);
    }

    #[test]
    fn test_mmap_read() {
        let mm = new_mmap_mm();
        assert_eq!(mm.read_slice::<u8>(0, mm.len()), [1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_mmap_write() {
        let mut mm = new_mmap_mm();
        mm.write(0, &10u8 as *const u8, 1);
        assert_eq!(mm.read_slice::<u8>(0, mm.len()), [10, 2, 3, 4, 5, 6]);
        mm.write(1, &20u8 as *const u8, 1);
        assert_eq!(mm.read_slice::<u8>(0, mm.len()), [10, 20, 3, 4, 5, 6]);
        let mut mm = new_empty_mmap_mm();
        mm.resize(size_of::<usize>());
        mm.write(0, &1995usize as *const usize, 1);
        assert_eq!(mm.read_slice::<usize>(0, 1), [1995]);
    }

    #[test]
    fn test_mmap_shrink_expand() {
        let mut mm = new_mmap_mm();
        mm.resize(3);
        assert_eq!(mm.read_slice::<u8>(0, mm.len()), [1, 2, 3]);
        mm.resize(6);
        assert_eq!(mm.read_slice::<u8>(0, mm.len()), [1, 2, 3, 0, 0, 0]);
        mm.resize(0);
        assert_eq!(mm.read_slice::<u8>(0, mm.len()), []);
        mm.resize(3);
        assert_eq!(mm.read_slice::<u8>(0, mm.len()), [0, 0, 0]);
    }

    #[test]
    fn test_sink() {
        let mut mm = MemoryManager::Sink;
        assert_eq!(mm.len(), 0);
        assert_eq!(mm.read_slice::<u8>(0, 0), []);
        let data = [1, 2, 3, 4, 5, 6];
        mm.write(99999, data.as_ptr(), data.len());
        mm.resize(1000);
        mm.resize(0);
    }
}
