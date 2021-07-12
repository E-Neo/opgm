use log::info;
use memmap::MmapMut;
use rayon::slice::ParallelSliceMut;
use std::{mem::size_of, ops::Shl};

pub struct GroupBy<'a, T, F, K>
where
    T: 'a,
    F: FnMut(&T) -> K,
    K: PartialEq,
{
    slice: &'a [T],
    key: F,
}

impl<'a, T, F, K> GroupBy<'a, T, F, K>
where
    T: 'a,
    F: FnMut(&T) -> K,
    K: PartialEq,
{
    pub fn new(slice: &'a [T], key: F) -> Self {
        GroupBy { slice, key }
    }
}

impl<'a, T, F, K> Iterator for GroupBy<'a, T, F, K>
where
    T: 'a,
    F: FnMut(&T) -> K,
    K: PartialEq,
{
    type Item = (K, &'a [T]);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.slice.is_empty() {
            None
        } else {
            let mut len = 1;
            let mut iter = self.slice.iter();
            let key = (self.key)(iter.next().unwrap());
            while let Some(x) = iter.next() {
                if key == (self.key)(x) {
                    len += 1;
                } else {
                    break;
                }
            }
            let (head, tail) = self.slice.split_at(len);
            self.slice = tail;
            Some((key, head))
        }
    }
}

pub struct ExactSizeIter<I: Iterator> {
    iter: I,
    len: usize,
}

impl<I: Iterator> ExactSizeIter<I> {
    pub fn new(iter: I, len: usize) -> Self {
        Self { iter, len }
    }
}

impl<T, I: Iterator<Item = T>> Iterator for ExactSizeIter<I> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<I: Iterator> ExactSizeIterator for ExactSizeIter<I> {
    fn len(&self) -> usize {
        self.len
    }
}

pub fn parallel_external_sort<T: Send + Ord>(data: &mut [T]) -> std::io::Result<()> {
    let mut chunk_size =
        512 * (sys_info::mem_info().unwrap().avail & u64::MAX.shl(20)) as usize / size_of::<T>();
    info!(
        "chunk_size = {}G",
        chunk_size * size_of::<T>() / 1024 / 1024 / 1024
    );
    for chunk in data.chunks_mut(chunk_size) {
        chunk.par_sort_unstable();
    }
    let len = data.len();
    if chunk_size < len {
        info!("merging...");
        let file = tempfile::tempfile()?;
        file.set_len((len * size_of::<T>()) as u64)?;
        let mut buf = unsafe { MmapMut::map_mut(&file)? };
        while chunk_size < len {
            let mut chunk_begin = 0;
            while chunk_begin < len {
                merge(
                    &mut data[chunk_begin..std::cmp::min(chunk_begin + 2 * chunk_size, len)],
                    std::cmp::min(chunk_size, len - chunk_begin),
                    buf.as_mut_ptr() as *mut T,
                    &mut (|a, b| a < b),
                );
                chunk_begin += 2 * chunk_size;
            }
            chunk_size *= 2;
        }
    }
    Ok(())
}

/// Merges non-decreasing runs `v[..mid]` and `v[mid..]` using `buf` as temporary storage, and
/// stores the result into `v[..]`.
///
/// https://github.com/rust-lang/rust/blob/481971978fda83aa7cf1f1f3c80cfad822377cf2
/// /library/alloc/src/slice.rs#L928-L1042
fn merge<T, F>(v: &mut [T], mid: usize, buf: *mut T, is_less: &mut F)
where
    F: FnMut(&T, &T) -> bool,
{
    let len = v.len();
    let v = v.as_mut_ptr();
    let (v_mid, v_end) = unsafe { (v.add(mid), v.add(len)) };

    // The merge process first copies the shorter run into `buf`. Then it traces the newly copied
    // run and the longer run forwards (or backwards), comparing their next unconsumed elements and
    // copying the lesser (or greater) one into `v`.
    //
    // As soon as the shorter run is fully consumed, the process is done. If the longer run gets
    // consumed first, then we must copy whatever is left of the shorter run into the remaining
    // hole in `v`.
    //
    // Intermediate state of the process is always tracked by `hole`, which serves two purposes:
    // 1. Protects integrity of `v` from panics in `is_less`.
    // 2. Fills the remaining hole in `v` if the longer run gets consumed first.
    //
    // Panic safety:
    //
    // If `is_less` panics at any point during the process, `hole` will get dropped and fill the
    // hole in `v` with the unconsumed range in `buf`, thus ensuring that `v` still holds every
    // object it initially held exactly once.
    let mut hole;

    if mid <= len - mid {
        // The left run is shorter.
        unsafe {
            std::ptr::copy_nonoverlapping(v, buf, mid);
            hole = MergeHole {
                start: buf,
                end: buf.add(mid),
                dest: v,
            };
        }

        // Initially, these pointers point to the beginnings of their arrays.
        let left = &mut hole.start;
        let mut right = v_mid;
        let out = &mut hole.dest;

        while *left < hole.end && right < v_end {
            // Consume the lesser side.
            // If equal, prefer the left run to maintain stability.
            unsafe {
                let to_copy = if is_less(&*right, &**left) {
                    get_and_increment(&mut right)
                } else {
                    get_and_increment(left)
                };
                std::ptr::copy_nonoverlapping(to_copy, get_and_increment(out), 1);
            }
        }
    } else {
        // The right run is shorter.
        unsafe {
            std::ptr::copy_nonoverlapping(v_mid, buf, len - mid);
            hole = MergeHole {
                start: buf,
                end: buf.add(len - mid),
                dest: v_mid,
            };
        }

        // Initially, these pointers point past the ends of their arrays.
        let left = &mut hole.dest;
        let right = &mut hole.end;
        let mut out = v_end;

        while v < *left && buf < *right {
            // Consume the greater side.
            // If equal, prefer the right run to maintain stability.
            unsafe {
                let to_copy = if is_less(&*right.offset(-1), &*left.offset(-1)) {
                    decrement_and_get(left)
                } else {
                    decrement_and_get(right)
                };
                std::ptr::copy_nonoverlapping(to_copy, decrement_and_get(&mut out), 1);
            }
        }
    }
    // Finally, `hole` gets dropped. If the shorter run was not fully consumed, whatever remains of
    // it will now be copied into the hole in `v`.

    fn get_and_increment<T>(ptr: &mut *mut T) -> *mut T {
        let old = *ptr;
        *ptr = unsafe { ptr.offset(1) };
        old
    }

    fn decrement_and_get<T>(ptr: &mut *mut T) -> *mut T {
        *ptr = unsafe { ptr.offset(-1) };
        *ptr
    }

    // When dropped, copies the range `start..end` into `dest..`.
    struct MergeHole<T> {
        start: *mut T,
        end: *mut T,
        dest: *mut T,
    }

    impl<T> Drop for MergeHole<T> {
        fn drop(&mut self) {
            // `T` is not a zero-sized type, so it's okay to divide by its size.
            let len = (self.end as usize - self.start as usize) / size_of::<T>();
            unsafe {
                std::ptr::copy_nonoverlapping(self.start, self.dest, len);
            }
        }
    }
}
