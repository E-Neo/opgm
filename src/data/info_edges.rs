use crate::{
    memory_manager::MemoryManager,
    tools::{parallel_external_sort, ExactSizeIter},
    types::{ELabel, VId, VLabel},
};
use log::info;
use std::{
    collections::{HashMap, HashSet},
    mem::size_of,
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
    let mut elabels: HashSet<ELabel> = HashSet::new();
    let mut pos = size_of::<InfoEdgeHeader>();
    info!("scanning edges...");
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
        pos += 2 * size_of::<InfoEdge>();
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
    info!("sorting...");
    parallel_external_sort(data).unwrap();
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
