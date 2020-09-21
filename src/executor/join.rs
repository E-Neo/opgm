use crate::{
    executor::{
        read_super_row_header, write_num_bytes, write_super_row_header, SuperRow, SuperRows,
    },
    memory_manager::MemoryManager,
    types::{GlobalConstraint, PosLen, SuperRowHeader, VId, VIdPos, VertexCoverConstraint},
};
use std::cmp::Ordering;
use std::mem::size_of;

/// Join two `SuperRow` files.
///
/// We adopt a simple index-based nested-loop join to join two `SuperRow` files,
/// Since the data are compressed, the test equality process in the conventional join process is
/// replaced by the set intersection operation.
/// And we could intersect two sets sequentially for free, since the elements are always sorted
/// in a `SuperRow` as a consequence of our well-designed `DataGraph` format.
/// The join process is always boosted by the index because the image of a vertex cover always
/// contains only one item, which could then be used as the key to run the binary search.
pub fn join(
    super_row_mm: &mut MemoryManager,
    num_eqvs: usize,
    num_cover: usize,
    left_super_row_mm: &MemoryManager,
    right_super_row_mm: &MemoryManager,
    index_mm: &MemoryManager,
    vertex_cover_constraint: &Option<GlobalConstraint>,
    indexed_intersection: usize,
    sequential_intersections: &[(usize, usize, Option<VertexCoverConstraint>)],
    left_keeps: &[(usize, Option<VertexCoverConstraint>)],
    right_keeps: &[(usize, Option<VertexCoverConstraint>)],
) {
    if let Some(vcgc) = vertex_cover_constraint {
        do_join_with_vcgc(
            super_row_mm,
            num_eqvs,
            num_cover,
            left_super_row_mm,
            right_super_row_mm,
            index_mm,
            vcgc,
            indexed_intersection,
            sequential_intersections,
            left_keeps,
            right_keeps,
        );
    } else {
        do_join(
            super_row_mm,
            num_eqvs,
            num_cover,
            left_super_row_mm,
            right_super_row_mm,
            index_mm,
            indexed_intersection,
            sequential_intersections,
            left_keeps,
            right_keeps,
        );
    }
}

/// Do join without vertex cover's global constraint.
fn do_join(
    super_row_mm: &mut MemoryManager,
    num_eqvs: usize,
    num_cover: usize,
    left_super_row_mm: &MemoryManager,
    right_super_row_mm: &MemoryManager,
    index_mm: &MemoryManager,
    indexed_intersection: usize,
    sequential_intersections: &[(usize, usize, Option<VertexCoverConstraint>)],
    left_keeps: &[(usize, Option<VertexCoverConstraint>)],
    right_keeps: &[(usize, Option<VertexCoverConstraint>)],
) {
    let (_, right_num_eqvs, _) = read_super_row_header(right_super_row_mm);
    let index = index_mm.read_slice::<VIdPos>(0, index_mm.len() / size_of::<VIdPos>());
    super_row_mm.resize(size_of::<SuperRowHeader>());
    let mut num_rows = 0;
    for left_sr in SuperRows::new(left_super_row_mm) {
        for &root in left_sr.images()[indexed_intersection] {
            if let Some(right_sr_pos) = index_get_pos(index, root) {
                let vc = get_vertex_cover(num_cover, &left_sr, root);
                let right_sr = SuperRow::new(right_super_row_mm, right_sr_pos, right_num_eqvs);
                join_two_super_rows(
                    super_row_mm,
                    num_eqvs,
                    num_cover,
                    &vc,
                    &left_sr,
                    &right_sr,
                    root,
                    sequential_intersections,
                    left_keeps,
                    right_keeps,
                );
                num_rows += 1;
            }
        }
    }
    write_super_row_header(super_row_mm, num_rows, num_eqvs, num_cover);
}

/// Do join with vertex cover's global constraint.
fn do_join_with_vcgc(
    super_row_mm: &mut MemoryManager,
    num_eqvs: usize,
    num_cover: usize,
    left_super_row_mm: &MemoryManager,
    right_super_row_mm: &MemoryManager,
    index_mm: &MemoryManager,
    vcgc: &GlobalConstraint,
    indexed_intersection: usize,
    sequential_intersections: &[(usize, usize, Option<VertexCoverConstraint>)],
    left_keeps: &[(usize, Option<VertexCoverConstraint>)],
    right_keeps: &[(usize, Option<VertexCoverConstraint>)],
) {
    let (_, right_num_eqvs, _) = read_super_row_header(right_super_row_mm);
    let index = index_mm.read_slice::<VIdPos>(0, index_mm.len() / size_of::<VIdPos>());
    super_row_mm.resize(size_of::<SuperRowHeader>());
    let mut num_rows = 0;
    for left_sr in SuperRows::new(left_super_row_mm) {
        for &root in left_sr.images()[indexed_intersection] {
            if let Some(right_sr_pos) = index_get_pos(index, root) {
                let vc = get_vertex_cover(num_cover, &left_sr, root);
                if vcgc.f()(&vc) {
                    let right_sr = SuperRow::new(right_super_row_mm, right_sr_pos, right_num_eqvs);
                    join_two_super_rows(
                        super_row_mm,
                        num_eqvs,
                        num_cover,
                        &vc,
                        &left_sr,
                        &right_sr,
                        root,
                        sequential_intersections,
                        left_keeps,
                        right_keeps,
                    );
                    num_rows += 1;
                }
            }
        }
    }
    write_super_row_header(super_row_mm, num_rows, num_eqvs, num_cover);
}

fn get_vertex_cover(num_cover: usize, left_sr: &SuperRow, root: VId) -> Vec<VId> {
    let mut vc = Vec::with_capacity(num_cover);
    for eqv in 0..num_cover - 1 {
        vc.push(left_sr.images()[eqv][0]);
    }
    vc.push(root);
    vc
}

/// Search the `sr_pos` of `root`.
fn index_get_pos(index: &[VIdPos], root: VId) -> Option<usize> {
    index
        .binary_search_by_key(&root, |vid_pos| vid_pos.vid)
        .ok()
        .map(|id| index[id].pos)
}

/// Tries to join two `SuperRow`s.
fn join_two_super_rows(
    super_row_mm: &mut MemoryManager,
    num_eqvs: usize,
    num_cover: usize,
    vertex_cover: &[VId],
    left_sr: &SuperRow,
    right_sr: &SuperRow,
    root: VId,
    sequential_intersections: &[(usize, usize, Option<VertexCoverConstraint>)],
    left_keeps: &[(usize, Option<VertexCoverConstraint>)],
    right_keeps: &[(usize, Option<VertexCoverConstraint>)],
) {
    let sr_pos = super_row_mm.len();
    super_row_mm.resize(
        sr_pos
            + estimate_num_bytes(
                num_eqvs,
                num_cover,
                left_sr,
                right_sr,
                sequential_intersections,
                left_keeps,
                right_keeps,
            ),
    );
    let mut pos = sr_pos + size_of::<usize>() + num_eqvs * size_of::<PosLen>();
    let mut pos_lens = Vec::with_capacity(num_eqvs);
    write_left_vertex_cover(
        super_row_mm,
        &mut pos,
        &mut pos_lens,
        left_sr,
        num_cover - 1,
    );
    write_right_root(super_row_mm, &mut pos, &mut pos_lens, root);
    write_sequential_intersections(
        super_row_mm,
        &mut pos,
        &mut pos_lens,
        vertex_cover,
        left_sr,
        right_sr,
        sequential_intersections,
    );
    write_keeps(
        super_row_mm,
        &mut pos,
        &mut pos_lens,
        vertex_cover,
        left_sr,
        left_keeps,
    );
    write_keeps(
        super_row_mm,
        &mut pos,
        &mut pos_lens,
        vertex_cover,
        right_sr,
        right_keeps,
    );
    super_row_mm.resize(pos);
    write_num_bytes(super_row_mm, sr_pos, pos - sr_pos);
    write_pos_lens(super_row_mm, sr_pos, pos_lens.as_slice());
}

/// Estimates the upper bound of the size of the new `SuperRow`.
fn estimate_num_bytes(
    num_eqvs: usize,
    num_cover: usize,
    left_sr: &SuperRow,
    right_sr: &SuperRow,
    sequential_intersections: &[(usize, usize, Option<VertexCoverConstraint>)],
    left_keeps: &[(usize, Option<VertexCoverConstraint>)],
    right_keeps: &[(usize, Option<VertexCoverConstraint>)],
) -> usize {
    size_of::<usize>()
        + num_eqvs * size_of::<PosLen>()
        + (num_cover
            + sequential_intersections
                .iter()
                .map(|&(l, r, _)| {
                    std::cmp::min(left_sr.images()[l].len(), right_sr.images()[r].len())
                })
                .sum::<usize>()
            + left_keeps
                .iter()
                .map(|&(l, _)| left_sr.images()[l].len())
                .sum::<usize>()
            + right_keeps
                .iter()
                .map(|&(r, _)| right_sr.images()[r].len())
                .sum::<usize>())
            * size_of::<VId>()
}

fn write_left_vertex_cover(
    super_row_mm: &mut MemoryManager,
    pos: &mut usize,
    pos_lens: &mut Vec<PosLen>,
    left_sr: &SuperRow,
    left_num_cover: usize,
) {
    for eqv in 0..left_num_cover {
        super_row_mm.write(*pos, &left_sr.images()[eqv][0] as *const _, 1);
        pos_lens.push(PosLen { pos: *pos, len: 1 });
        *pos += size_of::<VId>();
    }
}

fn write_right_root(
    super_row_mm: &mut MemoryManager,
    pos: &mut usize,
    pos_lens: &mut Vec<PosLen>,
    root: VId,
) {
    super_row_mm.write(*pos, &root as *const _, 1);
    pos_lens.push(PosLen { pos: *pos, len: 1 });
    *pos += size_of::<VId>();
}

fn write_sequential_intersections(
    super_row_mm: &mut MemoryManager,
    pos: &mut usize,
    pos_lens: &mut Vec<PosLen>,
    vertex_cover: &[VId],
    left_sr: &SuperRow,
    right_sr: &SuperRow,
    sequential_intersections: &[(usize, usize, Option<VertexCoverConstraint>)],
) {
    sequential_intersections.iter().for_each(|(l, r, vcc)| {
        let (left_image, right_image) = (left_sr.images()[*l], right_sr.images()[*r]);
        if let Some(vcc) = vcc {
            write_one_sequential_intersection_with_vcc(
                super_row_mm,
                pos,
                pos_lens,
                left_image,
                right_image,
                vertex_cover,
                vcc,
            );
        } else {
            write_one_sequential_intersection(super_row_mm, pos, pos_lens, left_image, right_image);
        }
    });
}

fn write_one_sequential_intersection(
    super_row_mm: &mut MemoryManager,
    pos: &mut usize,
    pos_lens: &mut Vec<PosLen>,
    left_image: &[VId],
    right_image: &[VId],
) {
    let mut new_pos = *pos;
    let (mut left_iter, mut right_iter) = (left_image.iter(), right_image.iter());
    let (mut left, mut right) = (left_iter.next(), right_iter.next());
    while let (Some(x), Some(y)) = (left, right) {
        match x.cmp(y) {
            Ordering::Less => left = left_iter.next(),
            Ordering::Equal => {
                super_row_mm.write(new_pos, x as *const VId, 1);
                new_pos += size_of::<VId>();
                left = left_iter.next();
                right = right_iter.next();
            }
            Ordering::Greater => right = right_iter.next(),
        }
    }
    pos_lens.push(PosLen {
        pos: *pos,
        len: (new_pos - *pos) / size_of::<VId>(),
    });
    *pos = new_pos;
}

fn write_one_sequential_intersection_with_vcc(
    super_row_mm: &mut MemoryManager,
    pos: &mut usize,
    pos_lens: &mut Vec<PosLen>,
    left_image: &[VId],
    right_image: &[VId],
    vertex_cover: &[VId],
    vcc: &VertexCoverConstraint,
) {
    let mut new_pos = *pos;
    let (mut left_iter, mut right_iter) = (left_image.iter(), right_image.iter());
    let (mut left, mut right) = (left_iter.next(), right_iter.next());
    while let (Some(x), Some(y)) = (left, right) {
        match x.cmp(y) {
            Ordering::Less => left = left_iter.next(),
            Ordering::Equal => {
                if vcc.f()(vertex_cover, *x) {
                    super_row_mm.write(new_pos, x as *const VId, 1);
                    new_pos += size_of::<VId>();
                    left = left_iter.next();
                    right = right_iter.next();
                }
            }
            Ordering::Greater => right = right_iter.next(),
        }
    }
    pos_lens.push(PosLen {
        pos: *pos,
        len: (new_pos - *pos) / size_of::<VId>(),
    });
    *pos = new_pos;
}

fn write_keeps(
    super_row_mm: &mut MemoryManager,
    pos: &mut usize,
    pos_lens: &mut Vec<PosLen>,
    vertex_cover: &[VId],
    keep_sr: &SuperRow,
    keeps: &[(usize, Option<VertexCoverConstraint>)],
) {
    keeps.iter().for_each(|(k, vcc)| {
        let image = keep_sr.images()[*k];
        if let Some(vcc) = vcc {
            write_keep_with_vcc(super_row_mm, pos, pos_lens, image, vertex_cover, vcc);
        } else {
            write_keep(super_row_mm, pos, pos_lens, image);
        }
    });
}

fn write_keep(
    super_row_mm: &mut MemoryManager,
    pos: &mut usize,
    pos_lens: &mut Vec<PosLen>,
    image: &[VId],
) {
    super_row_mm.write(*pos, image.as_ptr(), image.len());
    pos_lens.push(PosLen {
        pos: *pos,
        len: image.len(),
    });
    *pos += image.len() * size_of::<VId>();
}

fn write_keep_with_vcc(
    super_row_mm: &mut MemoryManager,
    pos: &mut usize,
    pos_lens: &mut Vec<PosLen>,
    image: &[VId],
    vertex_cover: &[VId],
    vcc: &VertexCoverConstraint,
) {
    let old_pos = *pos;
    for &v in image {
        if vcc.f()(vertex_cover, v) {
            super_row_mm.write(*pos, &v as *const VId, 1);
            *pos += size_of::<VId>();
        }
    }
    pos_lens.push(PosLen {
        pos: old_pos,
        len: (*pos - old_pos) / size_of::<VId>(),
    });
}

fn write_pos_lens(super_row_mm: &mut MemoryManager, sr_pos: usize, pos_lens: &[PosLen]) {
    super_row_mm.write(
        sr_pos + size_of::<usize>(),
        pos_lens.as_ptr(),
        pos_lens.len(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::super_row::{add_super_row, add_super_row_and_index, empty_super_row_mm};

    #[test]
    fn test_index_get_pos() {
        let index: Vec<VIdPos> = vec![(1, 2), (3, 4), (5, 6)]
            .into_iter()
            .map(|(vid, pos)| VIdPos { vid, pos })
            .collect();
        assert_eq!(index_get_pos(index.as_slice(), 1), Some(2));
        assert_eq!(index_get_pos(index.as_slice(), 2), None);
    }

    #[test]
    fn test_join() {
        let mut sr1_mm = MemoryManager::Mem(vec![]);
        empty_super_row_mm(&mut sr1_mm, 3, 1);
        add_super_row(&mut sr1_mm, &[1, 1, 1], &[&[0], &[1], &[5]]);
        add_super_row(&mut sr1_mm, &[1, 1, 1], &[&[3], &[4], &[2]]);
        let mut sr2_mm = MemoryManager::Mem(vec![]);
        let mut index_mm = MemoryManager::Mem(vec![]);
        empty_super_row_mm(&mut sr2_mm, 2, 1);
        add_super_row_and_index(&mut sr2_mm, &mut index_mm, &[1, 2], &[&[1], &[2, 5]]);
        add_super_row_and_index(&mut sr2_mm, &mut index_mm, &[1, 2], &[&[4], &[2, 5]]);
        let mut sr_mm = MemoryManager::Mem(vec![]);
        join(
            &mut sr_mm,
            4,
            2,
            &sr1_mm,
            &sr2_mm,
            &index_mm,
            &None,
            1,
            &[(2, 1, None)],
            &[],
            &[(1, None)],
        );
        let mut sr_mm1 = MemoryManager::Mem(vec![]);
        empty_super_row_mm(&mut sr_mm1, 4, 2);
        add_super_row(&mut sr_mm1, &[1, 1, 1, 2], &[&[0], &[1], &[5], &[2, 5]]);
        add_super_row(&mut sr_mm1, &[1, 1, 1, 2], &[&[3], &[4], &[2], &[2, 5]]);
        assert_eq!(
            sr_mm.read_slice::<u8>(0, sr_mm.len()),
            sr_mm1.read_slice::<u8>(0, sr_mm1.len())
        );
    }
}
