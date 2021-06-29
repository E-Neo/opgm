use memmap::{Mmap, MmapMut};
use std::fs::{File, OpenOptions};
use std::path::Path;

/// A read-only memory mapped file.
pub struct MmapFile {
    mmap: Mmap,
}

impl MmapFile {
    fn len(&self) -> usize {
        self.mmap.len()
    }

    fn read<T>(&self, pos: usize) -> *const T {
        unsafe { self.mmap.as_ptr().add(pos) as *const T }
    }
}

/// A memory mapped file.
pub struct MmapMutFile {
    file: File,
    mmap: MmapMut,
    len: u64,
}

impl MmapMutFile {
    fn from_file(file: File) -> Self {
        let len = file.metadata().unwrap().len();
        let mmap = if len == 0 {
            MmapMut::map_anon(1).unwrap()
        } else {
            unsafe { MmapMut::map_mut(&file).unwrap() }
        };
        Self { file, mmap, len }
    }

    fn len(&self) -> usize {
        self.len as usize
    }

    fn resize(&mut self, new_len: usize) {
        self.len = new_len as u64;
        self.mmap = MmapMut::map_anon(1).unwrap();
        self.file.set_len(self.len).unwrap();
        if new_len != 0 {
            self.mmap = unsafe { MmapMut::map_mut(&self.file).unwrap() }
        }
    }

    fn read<T>(&self, pos: usize) -> *const T {
        unsafe { self.mmap.as_ptr().add(pos) as *const T }
    }
}

/// A memory manager to hide the underlying type of the memory buffer.
pub enum MemoryManager {
    /// A memory buffer.
    Mem(Vec<u8>),
    /// A read-only memory mapped buffer.
    Mmap(MmapFile),
    /// A memory mapped buffer.
    MmapMut(MmapMutFile),
    /// A sink.
    Sink,
}

impl MemoryManager {
    pub fn new_mem(size: usize) -> Self {
        MemoryManager::Mem(vec![0; size])
    }

    pub fn new_mmap<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        Ok(MemoryManager::Mmap(MmapFile {
            mmap: unsafe { Mmap::map(&File::open(path)?)? },
        }))
    }

    pub fn new_mmap_mut<P: AsRef<Path>>(path: P, size: usize) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.set_len(size as u64)?;
        Ok(MemoryManager::MmapMut(MmapMutFile::from_file(file)))
    }

    pub fn new_sink() -> Self {
        MemoryManager::Sink
    }

    pub fn len(&self) -> usize {
        match self {
            MemoryManager::Mem(vec) => vec.len(),
            MemoryManager::Mmap(mmapfile) => mmapfile.len(),
            MemoryManager::MmapMut(mmapfile) => mmapfile.len(),
            MemoryManager::Sink => usize::MAX,
        }
    }

    pub fn resize(&mut self, new_len: usize) {
        match self {
            MemoryManager::Mem(vec) => vec.resize(new_len, 0),
            MemoryManager::Mmap(_) => unimplemented!(),
            MemoryManager::MmapMut(mmapfile) => mmapfile.resize(new_len),
            MemoryManager::Sink => (),
        }
    }

    pub unsafe fn as_ref<T>(&self, pos: usize) -> &T {
        match self {
            MemoryManager::Mem(vec) => (vec.as_ptr().add(pos) as *const T).as_ref().unwrap(),
            MemoryManager::Mmap(mmapfile) => (mmapfile.mmap.as_ptr().add(pos) as *const T)
                .as_ref()
                .unwrap(),
            MemoryManager::MmapMut(mmapfile) => (mmapfile.mmap.as_ptr().add(pos) as *const T)
                .as_ref()
                .unwrap(),
            MemoryManager::Sink => unimplemented!(),
        }
    }

    pub unsafe fn as_slice<T>(&self, pos: usize, count: usize) -> &[T] {
        match self {
            MemoryManager::Mem(vec) => {
                std::slice::from_raw_parts(vec.as_ptr().add(pos) as *const T, count)
            }
            MemoryManager::Mmap(mmapfile) => std::slice::from_raw_parts(mmapfile.read(pos), count),
            MemoryManager::MmapMut(mmapfile) => {
                std::slice::from_raw_parts(mmapfile.read(pos), count)
            }
            MemoryManager::Sink => unimplemented!(),
        }
    }

    pub unsafe fn as_mut_slice<T>(&mut self, pos: usize, count: usize) -> &mut [T] {
        match self {
            MemoryManager::Mem(vec) => {
                std::slice::from_raw_parts_mut(vec.as_mut_ptr().add(pos) as *mut T, count)
            }
            MemoryManager::MmapMut(mmapfile) => {
                std::slice::from_raw_parts_mut(mmapfile.mmap.as_mut_ptr().add(pos) as *mut T, count)
            }
            _ => unimplemented!(),
        }
    }

    pub unsafe fn copy_from_slice<T>(&mut self, pos: usize, src: &[T]) {
        match self {
            MemoryManager::Mem(vec) => {
                std::ptr::copy(src.as_ptr(), vec.as_mut_ptr().add(pos) as *mut T, src.len())
            }
            MemoryManager::Mmap(_) => unimplemented!(),
            MemoryManager::MmapMut(mmapfile) => std::ptr::copy(
                src.as_ptr(),
                mmapfile.mmap.as_mut_ptr().add(pos) as *mut T,
                src.len(),
            ),
            MemoryManager::Sink => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

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
        assert_eq!(
            unsafe { mm.as_slice::<u8>(0, mm.len()) },
            [1, 2, 3, 4, 5, 6]
        );
    }

    #[test]
    fn test_mem_shrink_expand() {
        let mut mm = MemoryManager::Mem(vec![1, 2, 3, 4, 5, 6]);
        mm.resize(3);
        assert_eq!(unsafe { mm.as_slice::<u8>(0, mm.len()) }, [1, 2, 3]);
        mm.resize(6);
        assert_eq!(
            unsafe { mm.as_slice::<u8>(0, mm.len()) },
            [1, 2, 3, 0, 0, 0]
        );
        mm.resize(0);
        assert_eq!(unsafe { mm.as_slice::<u8>(0, mm.len()) }, []);
        mm.resize(3);
        assert_eq!(unsafe { mm.as_slice::<u8>(0, mm.len()) }, [0, 0, 0]);
    }

    fn new_mmap_mm() -> MemoryManager {
        let mut file = tempfile::tempfile().unwrap();
        file.write(&[1, 2, 3, 4, 5, 6]).unwrap();
        let mmapfile = MmapMutFile::from_file(file);
        MemoryManager::MmapMut(mmapfile)
    }

    fn new_empty_mmap_mm() -> MemoryManager {
        let file = tempfile::tempfile().unwrap();
        let mmapfile = MmapMutFile::from_file(file);
        MemoryManager::MmapMut(mmapfile)
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
        assert_eq!(
            unsafe { mm.as_slice::<u8>(0, mm.len()) },
            [1, 2, 3, 4, 5, 6]
        );
    }

    #[test]
    fn test_mmap_shrink_expand() {
        let mut mm = new_mmap_mm();
        mm.resize(3);
        assert_eq!(unsafe { mm.as_slice::<u8>(0, mm.len()) }, [1, 2, 3]);
        mm.resize(6);
        assert_eq!(
            unsafe { mm.as_slice::<u8>(0, mm.len()) },
            [1, 2, 3, 0, 0, 0]
        );
        mm.resize(0);
        assert_eq!(unsafe { mm.as_slice::<u8>(0, mm.len()) }, []);
        mm.resize(3);
        assert_eq!(unsafe { mm.as_slice::<u8>(0, mm.len()) }, [0, 0, 0]);
    }

    #[test]
    fn test_mem() {
        let mut mm = MemoryManager::new_mem(29);
        assert_eq!(mm.len(), 29);
        unsafe {
            mm.copy_from_slice::<i32>(1, &[3, 2, 1]);
        }
        assert_eq!(unsafe { mm.as_ref::<i32>(1) }, &3);
        assert_eq!(unsafe { mm.as_slice::<i32>(1, 3) }, &[3, 2, 1]);
        unsafe { mm.as_mut_slice::<i32>(1, 3) }.sort();
        assert_eq!(unsafe { mm.as_slice::<i32>(1, 3) }, &[1, 2, 3]);
        mm.resize(3 * size_of::<i32>());
        assert_eq!(unsafe { mm.as_slice::<i32>(1, 3) }, &[1, 2, 3]);
    }

    #[test]
    fn test_mmap() {
        let mut file = NamedTempFile::new().unwrap();
        let data: &[i32] = &[3, 2, 1];
        file.write_all(unsafe {
            std::slice::from_raw_parts(data.as_ptr() as *const u8, size_of::<i32>() * data.len())
        })
        .unwrap();
        let path = file.into_temp_path();
        let mm = MemoryManager::new_mmap(path).unwrap();
        assert_eq!(mm.len(), 3 * size_of::<i32>());
        assert_eq!(unsafe { mm.as_ref::<i32>(0) }, &3);
        assert_eq!(unsafe { mm.as_slice::<i32>(0, 3) }, &[3, 2, 1]);
    }

    #[test]
    fn test_mmap_mut() {
        let mut file = NamedTempFile::new().unwrap();
        let data: &[i32] = &[3, 2, 1];
        file.write_all(unsafe {
            std::slice::from_raw_parts(data.as_ptr() as *const u8, size_of::<i32>() * data.len())
        })
        .unwrap();
        let path = file.into_temp_path();
        let mut mm = MemoryManager::new_mmap_mut(path, 29).unwrap();
        assert_eq!(mm.len(), 29);
        assert_eq!(unsafe { mm.as_slice::<i32>(0, mm.len()) }, &[0; 29]);
        unsafe {
            mm.copy_from_slice::<i32>(1, &[3, 2, 1]);
        }
        assert_eq!(unsafe { mm.as_ref::<i32>(1) }, &3);
        assert_eq!(unsafe { mm.as_slice::<i32>(1, 3) }, &[3, 2, 1]);
        unsafe { mm.as_mut_slice::<i32>(1, 3) }.sort();
        assert_eq!(unsafe { mm.as_slice::<i32>(1, 3) }, &[1, 2, 3]);
        mm.resize(3 * size_of::<i32>());
        assert_eq!(unsafe { mm.as_slice::<i32>(1, 3) }, &[1, 2, 3]);
    }
}
