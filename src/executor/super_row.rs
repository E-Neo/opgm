use crate::{
    memory_manager::MemoryManager,
    types::{PosLen, SuperRowHeader, VId, VIdPos},
};
use itertools::Itertools;
use std::{collections::HashSet, mem::size_of};

pub struct SuperRow<'a> {
    images: Vec<&'a [VId]>,
}

impl<'a> SuperRow<'a> {
    pub fn new(mm: &'a MemoryManager, sr_pos: usize, num_eqvs: usize) -> Self {
        Self {
            images: read_pos_lens(mm, sr_pos, num_eqvs)
                .iter()
                .map(|pos_len| mm.read_slice::<VId>(pos_len.pos, pos_len.len))
                .collect(),
        }
    }

    pub fn images(&self) -> &[&'a [VId]] {
        self.images.as_slice()
    }

    pub fn num_eqvs(&self) -> usize {
        self.images.len()
    }

    pub fn num_bytes(&self) -> usize {
        unsafe {
            *((self.images[0].as_ptr() as *const u8)
                .sub(size_of::<PosLen>() * self.num_eqvs() + size_of::<usize>())
                as *const usize)
        }
    }

    pub fn count_rows(&self, vertex_eqv: &'a [(VId, usize)]) -> usize {
        vertex_eqv
            .iter()
            .map(|&(_, eqv)| self.images()[eqv].len())
            .product()
    }

    pub fn count_rows_slow(&self, vertex_eqv: &'a [(VId, usize)]) -> usize {
        let mut num_rows = 0;
        vertex_eqv
            .iter()
            .map(|&(_, eqv)| self.images()[eqv])
            .multi_cartesian_product()
            .for_each(|_| num_rows += 1);
        num_rows
    }

    /// Decompress the SuperRow.
    ///
    /// `vertex_eqv` should be sorted by `VId`.
    pub fn decompress(self, vertex_eqv: &'a [(VId, usize)]) -> SuperRowIter {
        let mappings = vertex_eqv
            .iter()
            .map(|(_, eqv)| self.images()[*eqv])
            .collect();
        SuperRowIter {
            mappings,
            mappings_offsets: vec![0; vertex_eqv.len()],
            row: Vec::with_capacity(vertex_eqv.len()),
            row_set: HashSet::with_capacity(vertex_eqv.len()),
        }
    }
}

impl<'a> std::fmt::Display for SuperRow<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.images)
    }
}

pub struct SuperRowIter<'a> {
    mappings: Vec<&'a [VId]>,
    mappings_offsets: Vec<usize>,
    row: Vec<VId>,
    row_set: HashSet<VId>,
}

impl<'a> Iterator for SuperRowIter<'a> {
    type Item = Vec<VId>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let col = self.row.len();
            if col == self.mappings.len() {
                let row = self.row.clone();
                if let Some(v) = self.row.pop() {
                    self.row_set.remove(&v);
                }
                return Some(row);
            } else if self.mappings_offsets[col] < self.mappings[col].len() {
                let v = self.mappings[col][self.mappings_offsets[col]];
                if self.row_set.insert(v) {
                    self.row.push(v);
                }
                self.mappings_offsets[col] += 1;
            } else {
                self.mappings_offsets[col] = 0;
                if let Some(v) = self.row.pop() {
                    self.row_set.remove(&v);
                } else {
                    return None;
                }
            }
        }
    }
}

pub struct SuperRows<'a> {
    mm: &'a MemoryManager,
    num_rows: usize,
    num_eqvs: usize,
    current_row: usize,
    sr_pos: usize,
}

impl<'a> SuperRows<'a> {
    pub fn new(mm: &'a MemoryManager) -> Self {
        let (num_rows, num_eqvs, _) = read_super_row_header(mm);
        Self {
            mm,
            num_rows,
            num_eqvs,
            current_row: 0,
            sr_pos: size_of::<SuperRowHeader>(),
        }
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}

impl<'a> Iterator for SuperRows<'a> {
    type Item = SuperRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_row < self.num_rows {
            let sr_pos = self.sr_pos;
            self.sr_pos += read_usize(self.mm, sr_pos);
            self.current_row += 1;
            Some(SuperRow::new(self.mm, sr_pos, self.num_eqvs))
        } else {
            None
        }
    }
}

pub struct SuperRowsInfo {
    num_rows: usize,
    num_eqvs: usize,
    num_vertices: usize,
    eqv_total_num_vertices: Vec<usize>,
}

impl SuperRowsInfo {
    pub fn new(mm: &MemoryManager) -> Self {
        let (num_rows, num_eqvs, num_vertices) = read_super_row_header(mm);
        let mut eqv_total_num_vertices = vec![0; num_eqvs];
        for sr in SuperRows::new(mm) {
            for (num, &img) in eqv_total_num_vertices.iter_mut().zip(sr.images()) {
                *num += img.len();
            }
        }
        Self {
            num_rows,
            num_eqvs,
            num_vertices,
            eqv_total_num_vertices,
        }
    }
}

impl std::fmt::Display for SuperRowsInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "num_rows: {}", self.num_rows)?;
        writeln!(f, "num_eqvs: {}", self.num_eqvs)?;
        writeln!(f, "num_vertices: {}", self.num_vertices)?;
        writeln!(
            f,
            "eqv_total_num_vertices: {:?}",
            self.eqv_total_num_vertices
        )?;
        writeln!(
            f,
            "eqv_avg_num_vertices: {:?}",
            self.eqv_total_num_vertices
                .iter()
                .map(|&num| (num as f64) / (self.num_rows as f64))
                .collect::<Vec<_>>()
        )?;
        Ok(())
    }
}

pub(crate) fn read_super_row_header(super_row_mm: &MemoryManager) -> (usize, usize, usize) {
    let header = unsafe { &*super_row_mm.read::<SuperRowHeader>(0) };
    (header.num_rows, header.num_eqvs, header.num_vertices)
}

fn read_pos_lens(super_row_mm: &MemoryManager, sr_pos: usize, num_eqvs: usize) -> &[PosLen] {
    super_row_mm.read_slice(sr_pos + size_of::<usize>(), num_eqvs)
}

fn read_usize(mm: &MemoryManager, pos: usize) -> usize {
    unsafe { *mm.read::<usize>(pos) }
}

pub(crate) fn write_super_row_header(
    super_row_mm: &mut MemoryManager,
    num_rows: usize,
    num_eqvs: usize,
    num_vertices: usize,
) {
    super_row_mm.write(
        0,
        &SuperRowHeader {
            num_rows,
            num_eqvs,
            num_vertices,
        } as *const SuperRowHeader,
        1,
    );
}

pub(crate) fn write_num_bytes(super_row_mm: &mut MemoryManager, sr_pos: usize, num_bytes: usize) {
    super_row_mm.write(sr_pos, &num_bytes as *const _, 1);
}

pub(crate) fn write_pos_len(
    super_row_mm: &mut MemoryManager,
    sr_pos: usize,
    eqv: usize,
    pos: usize,
    len: usize,
) {
    super_row_mm.write(
        sr_pos + size_of::<usize>() + eqv * size_of::<PosLen>(),
        &PosLen { pos, len } as *const _,
        1,
    );
}

pub(crate) fn write_vid(super_row_mm: &mut MemoryManager, pos: usize, vid: VId) {
    super_row_mm.write(pos, &vid as *const _, 1);
}

pub(crate) fn write_index(index_mm: &mut MemoryManager, idx_pos: usize, vid: VId, pos: usize) {
    index_mm.write(idx_pos, &VIdPos { vid, pos } as *const _, 1);
}

pub fn empty_super_row_mm(super_row_mm: &mut MemoryManager, num_eqvs: usize, num_vertices: usize) {
    super_row_mm.resize(size_of::<SuperRowHeader>());
    super_row_mm.write(
        0,
        &SuperRowHeader {
            num_rows: 0,
            num_eqvs,
            num_vertices,
        } as *const _,
        1,
    );
}

pub fn add_super_row(super_row_mm: &mut MemoryManager, bounds: &[usize], super_row: &[&[VId]]) {
    let header = unsafe { &*super_row_mm.read::<SuperRowHeader>(0) };
    let (num_rows, num_eqvs, num_vertices) =
        (header.num_rows, header.num_eqvs, header.num_vertices);
    let sr_pos = super_row_mm.len();
    let num_bytes = calculate_num_byte(num_eqvs, bounds);
    super_row_mm.resize(sr_pos + num_bytes);
    write_num_bytes(super_row_mm, sr_pos, num_bytes);
    let mut pos = sr_pos + size_of::<usize>() + num_eqvs * size_of::<PosLen>();
    for (eqv, (&bound, &img)) in bounds.iter().zip(super_row).enumerate() {
        super_row_mm.write(
            sr_pos + size_of::<usize>() + eqv * size_of::<PosLen>(),
            &PosLen {
                pos,
                len: img.len(),
            } as *const _,
            1,
        );
        super_row_mm.write(pos, img.as_ptr(), img.len());
        pos += bound * size_of::<VId>();
    }
    write_super_row_header(super_row_mm, num_rows + 1, num_eqvs, num_vertices);
}

pub fn add_super_row_and_index(
    super_row_mm: &mut MemoryManager,
    index_mm: &mut MemoryManager,
    bounds: &[usize],
    super_row: &[&[VId]],
) {
    let sr_pos = super_row_mm.len();
    add_super_row(super_row_mm, bounds, super_row);
    let idx_pos = index_mm.len();
    index_mm.resize(idx_pos + size_of::<VIdPos>());
    index_mm.write(
        idx_pos,
        &VIdPos {
            vid: *super_row.get(0).unwrap().get(0).unwrap(),
            pos: sr_pos,
        },
        1,
    );
}

pub fn add_super_row_and_index_compact(
    super_row_mm: &mut MemoryManager,
    index_mm: &mut MemoryManager,
    super_row: &[&[VId]],
) {
    add_super_row_and_index(
        super_row_mm,
        index_mm,
        &super_row.iter().map(|sr| sr.len()).collect::<Vec<usize>>(),
        super_row,
    );
}

fn calculate_num_byte(num_eqvs: usize, bounds: &[usize]) -> usize {
    size_of::<usize>()
        + num_eqvs * size_of::<PosLen>()
        + bounds.iter().sum::<usize>() * size_of::<VId>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_super_row_and_index() {
        let mut sr_mm = MemoryManager::Mem(vec![]);
        let mut index_mm = MemoryManager::Mem(vec![]);
        empty_super_row_mm(&mut sr_mm, 2, 1);
        add_super_row_and_index(&mut sr_mm, &mut index_mm, &[1, 2], &[&[1], &[2, 5]]);
        add_super_row_and_index(&mut sr_mm, &mut index_mm, &[1, 2], &[&[4], &[2, 5]]);
        assert_eq!(
            index_mm
                .read_slice::<VIdPos>(0, index_mm.len() / size_of::<VIdPos>())
                .iter()
                .map(|vid_pos| SuperRow::new(&sr_mm, vid_pos.pos, 2)
                    .images()
                    .iter()
                    .map(|image| image.to_vec())
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![vec![1], vec![2, 5]], vec![vec![4], vec![2, 5]]]
        );
    }
}
