use crate::{
    memory_manager::MemoryManager,
    types::{PosLen, SuperRowHeader, VId, VIdPos},
};
use std::mem::size_of;

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

pub(crate) fn read_super_row_header(super_row_mm: &MemoryManager) -> (usize, usize, usize) {
    let header = unsafe { &*super_row_mm.read::<SuperRowHeader>(0) };
    (header.num_rows, header.num_eqvs, header.num_cover)
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
    num_cover: usize,
) {
    super_row_mm.write(
        0,
        &SuperRowHeader {
            num_rows,
            num_eqvs,
            num_cover,
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

pub fn empty_super_row_mm(super_row_mm: &mut MemoryManager, num_eqvs: usize, num_cover: usize) {
    super_row_mm.resize(size_of::<SuperRowHeader>());
    super_row_mm.write(
        0,
        &SuperRowHeader {
            num_rows: 0,
            num_eqvs,
            num_cover,
        } as *const _,
        1,
    );
}

pub fn add_super_row(super_row_mm: &mut MemoryManager, bounds: &[usize], super_row: &[&[VId]]) {
    let header = unsafe { &*super_row_mm.read::<SuperRowHeader>(0) };
    let (num_rows, num_eqvs, num_cover) = (header.num_rows, header.num_eqvs, header.num_cover);
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
    write_super_row_header(super_row_mm, num_rows + 1, num_eqvs, num_cover);
}

fn calculate_num_byte(num_eqvs: usize, bounds: &[usize]) -> usize {
    size_of::<usize>()
        + num_eqvs * size_of::<PosLen>()
        + bounds.iter().sum::<usize>() * size_of::<VId>()
}
