use crate::{
    memory_manager::MemoryManager,
    types::{ELabel, VId, VLabel},
};
use itertools::Itertools;
use log::info;
use rayon::slice::ParallelSliceMut;
use std::{
    collections::{HashMap, HashSet},
    mem::size_of,
};

use super::types::{InfoEdge, InfoEdgeHeader};

type Vertex = (VId, VLabel);
type Edge = (VId, VId, ELabel);

pub fn mm_from_iter<V, E>(mm: &mut MemoryManager, vertices: V, edges: E)
where
    V: IntoIterator<Item = Vertex>,
    E: IntoIterator<Item = Edge>,
{
    info!("scanning vertices...");
    let (vid_vlabel_map, num_vlabels) = create_vid_vlabel_map(vertices);
    let block_size = 1024 * (sys_info::mem_info().unwrap().avail & 0xfffffffffff00000) as usize
        / size_of::<InfoEdge>();
    info!(
        "block_size={}M",
        block_size * size_of::<InfoEdge>() / 1024 / 1024
    );
    let mut buf_mm = MemoryManager::new_mem(block_size * size_of::<InfoEdge>());
    let mut buf_mm_pos = 0;
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let mut temp_mm = MemoryManager::new_mmap_mut(temp_file.into_temp_path(), 0).unwrap();
    let mut temp_mm_pos = 0;
    let mut elabels: HashSet<ELabel> = HashSet::new();
    info!("scanning edges...");
    let mut num_scanned = 0;
    for (src, dst, elabel) in edges {
        elabels.insert(elabel);
        unsafe {
            buf_mm.copy_from_slice(
                buf_mm_pos,
                &[
                    (
                        *vid_vlabel_map.get(&dst).unwrap(),
                        dst,
                        *vid_vlabel_map.get(&src).unwrap(),
                        src,
                        false,
                        elabel,
                    ),
                    (
                        *vid_vlabel_map.get(&src).unwrap(),
                        src,
                        *vid_vlabel_map.get(&dst).unwrap(),
                        dst,
                        true,
                        elabel,
                    ),
                ],
            );
        }
        num_scanned += 2;
        buf_mm_pos += 2 * size_of::<InfoEdge>();
        if num_scanned % block_size == 0 {
            sort_and_write(&mut buf_mm, &mut temp_mm, temp_mm_pos, block_size);
            buf_mm_pos = 0;
            temp_mm_pos += block_size * size_of::<InfoEdge>();
        }
    }
    if temp_mm_pos == 0 {
        sort_and_write(&mut buf_mm, mm, size_of::<InfoEdgeHeader>(), num_scanned);
    } else {
        if num_scanned * size_of::<InfoEdge>() > temp_mm_pos {
            sort_and_write(
                &mut buf_mm,
                &mut temp_mm,
                temp_mm_pos,
                num_scanned - temp_mm_pos / size_of::<InfoEdge>(),
            );
        }
        mm.resize(size_of::<InfoEdgeHeader>() + num_scanned * size_of::<InfoEdge>());
        let data: &[InfoEdge] = unsafe { temp_mm.as_slice(0, num_scanned) };
        info!("merging...");
        for (i, &info_edge) in data.chunks(block_size).kmerge().enumerate() {
            unsafe {
                mm.copy_from_slice(
                    size_of::<InfoEdgeHeader>() + i * size_of::<InfoEdge>(),
                    &[info_edge],
                );
            }
        }
    }
    unsafe {
        mm.copy_from_slice::<InfoEdgeHeader>(
            0,
            &[InfoEdgeHeader {
                num_vertices: vid_vlabel_map.len() as u32,
                num_edges: (num_scanned / 2) as u64,
                num_vlabels: num_vlabels as u16,
                num_elabels: elabels.len() as u16,
            }],
        );
    }
}

/// Reads the data graph stored in the SQLite3 file into `mm`.
///
/// The SQLite3 file must have the following schema:
///
/// ```sql
/// CREATE TABLE vertices (vid INT, vlabel INT);
/// CREATE TABLE edges (src INT, dst INT, elabel INT);
/// ```
pub fn mm_from_sqlite(mm: &mut MemoryManager, conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    let mut vertices_stmt = conn.prepare("SELECT * FROM vertices")?;
    let mut edges_stmt = conn.prepare("SELECT * FROM edges")?;
    mm_from_iter(
        mm,
        vertices_stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .filter_map(|row| row.ok()),
        edges_stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .filter_map(|row| row.ok()),
    );
    Ok(())
}

fn sort_and_write(temp_mm: &mut MemoryManager, mm: &mut MemoryManager, pos: usize, len: usize) {
    mm.resize(pos + len * size_of::<InfoEdge>());
    info!("sorting...");
    unsafe { temp_mm.as_mut_slice::<InfoEdge>(0, len) }.par_sort_unstable();
    info!("sorted");
    info!("writing...");
    unsafe {
        mm.copy_from_slice(pos, temp_mm.as_slice::<InfoEdge>(0, len));
    }
    info!("wrote");
}

fn create_vid_vlabel_map<V>(vertices: V) -> (HashMap<VId, VLabel>, usize)
where
    V: IntoIterator<Item = Vertex>,
{
    let mut label_set = HashSet::new();
    let mut vid_vlabel_map = HashMap::new();
    for (vid, vlabel) in vertices {
        label_set.insert(vlabel);
        vid_vlabel_map.insert(vid, vlabel);
    }
    (vid_vlabel_map, label_set.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mm_from_iter() {
        let mut mm = MemoryManager::new_mem(0);
        let info_edges: &[InfoEdge] = &[
            (1, 1, 2, 2, true, 12),
            (1, 1, 2, 3, true, 13),
            (2, 2, 1, 1, false, 12),
            (2, 2, 2, 3, true, 23),
            (2, 3, 1, 1, false, 13),
            (2, 3, 2, 2, false, 23),
        ];
        mm_from_iter(
            &mut mm,
            vec![(1, 1), (2, 2), (3, 2)].into_iter(),
            vec![(1, 2, 12), (1, 3, 13), (2, 3, 23)].into_iter(),
        );
        let &InfoEdgeHeader {
            num_vertices,
            num_edges,
            num_vlabels,
            num_elabels,
        } = unsafe { mm.as_ref::<InfoEdgeHeader>(0) };
        assert_eq!(
            (num_vertices, num_edges, num_vlabels, num_elabels),
            (3, 3, 2, 3)
        );
        assert_eq!(
            unsafe {
                mm.as_slice::<InfoEdge>(size_of::<InfoEdgeHeader>(), 2 * (num_edges as usize))
            },
            info_edges
        );
    }
}
