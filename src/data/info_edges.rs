use crate::{
    memory_manager::MemoryManager,
    tools::{merge, ExactSizeIter},
    types::{ELabel, VId, VLabel},
};
use log::info;
use memmap::MmapMut;
use rayon::slice::ParallelSliceMut;
use std::{
    collections::{HashMap, HashSet},
    mem::size_of,
    ops::Shl,
};

use super::types::{InfoEdge, InfoEdgeHeader};

type Vertex = (VId, VLabel);
type Edge = (VId, VId, ELabel);

pub fn mm_from_iter<V, E>(mm: &mut MemoryManager, vertices: V, edges: E)
where
    V: ExactSizeIterator<Item = Vertex>,
    E: ExactSizeIterator<Item = Edge>,
{
    let (num_vertices, num_edges) = (vertices.len(), edges.len());
    mm.resize(size_of::<InfoEdgeHeader>() + 2 * num_edges * size_of::<InfoEdge>());
    info!("scanning vertices...");
    let (vid_vlabel_map, num_vlabels) = create_vid_vlabel_map(vertices);
    let mut chunk_size = 512 * (sys_info::mem_info().unwrap().avail & u64::MAX.shl(20)) as usize
        / size_of::<InfoEdge>();
    info!(
        "chunk_size = {}G",
        chunk_size * size_of::<InfoEdge>() / 1024 / 1024 / 1024
    );
    let mut elabels: HashSet<ELabel> = HashSet::new();
    let mut pos = size_of::<InfoEdgeHeader>();
    let mut chunk_pos = pos;
    info!("scanning edges...");
    let mut num_scanned = 0;
    for (src, dst, elabel) in edges {
        elabels.insert(elabel);
        unsafe {
            mm.copy_from_slice(
                pos,
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
        pos += 2 * size_of::<InfoEdge>();
        if num_scanned % chunk_size == 0 {
            info!("sorting...");
            unsafe { mm.as_mut_slice::<InfoEdge>(chunk_pos, chunk_size) }.par_sort_unstable();
            chunk_pos = pos;
        }
    }
    if pos > chunk_pos {
        unsafe {
            mm.as_mut_slice::<InfoEdge>(chunk_pos, (pos - chunk_pos) / size_of::<InfoEdge>())
        }
        .par_sort_unstable();
    }
    unsafe {
        mm.copy_from_slice::<InfoEdgeHeader>(
            0,
            &[InfoEdgeHeader {
                num_vertices: num_vertices as u32,
                num_edges: num_edges as u64,
                num_vlabels: num_vlabels as u16,
                num_elabels: elabels.len() as u16,
            }],
        );
    }
    mm.resize(pos);
    let data: &mut [InfoEdge] =
        unsafe { mm.as_mut_slice(size_of::<InfoEdgeHeader>(), num_edges * 2) };
    let len = data.len();
    if chunk_size < len {
        info!(
            "chunk_size = {}G merging...",
            chunk_size * size_of::<InfoEdge>() / 1024 / 1024 / 1024
        );
        let file = tempfile::tempfile().unwrap();
        file.set_len((len * size_of::<InfoEdge>()) as u64).unwrap();
        let mut buf = unsafe { MmapMut::map_mut(&file).unwrap() };
        while chunk_size < len {
            let mut chunk_begin = 0;
            while chunk_begin < len {
                merge(
                    &mut data[chunk_begin..std::cmp::min(chunk_begin + 2 * chunk_size, len)],
                    std::cmp::min(chunk_size, len - chunk_begin),
                    buf.as_mut_ptr() as *mut InfoEdge,
                    &mut (|a, b| a < b),
                );
                chunk_begin += 2 * chunk_size;
            }
            chunk_size *= 2;
        }
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
    info!("quering num_vertices...");
    let num_vertices = conn
        .prepare("SELECT COUNT(*) FROM vertices")?
        .query_row([], |row| row.get(0))?;
    info!("quering num_edges...");
    let num_edges = conn
        .prepare("SELECT COUNT(*) FROM edges")?
        .query_row([], |row| row.get(0))?;
    let mut vertices_stmt = conn.prepare("SELECT * FROM vertices")?;
    let mut edges_stmt = conn.prepare("SELECT * FROM edges")?;
    mm_from_iter(
        mm,
        ExactSizeIter::new(
            vertices_stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .filter_map(|row| row.ok()),
            num_vertices,
        ),
        ExactSizeIter::new(
            edges_stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
                .filter_map(|row| row.ok()),
            num_edges,
        ),
    );
    Ok(())
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
