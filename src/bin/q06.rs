use clap::{App, Arg};
use opgm::{
    data_graph::DataGraph,
    executor::{match_characteristics, Index, Intersection, SuperRows},
    memory_manager::{MemoryManager, MmapReadOnlyFile},
    pattern_graph::{Characteristic, PatternGraph},
    planner::{CharacteristicInfo, IndexType},
};
use std::{collections::HashSet, error::Error, fs::File};

fn create_pattern<'a>() -> PatternGraph<'a> {
    let mut pattern_graph = PatternGraph::new();
    for (vid, vlabel) in vec![(1, 0), (2, 0), (3, 0), (4, 0)] {
        pattern_graph.add_vertex(vid, vlabel);
    }
    for (src, dst, elabel) in vec![
        (1, 2, 0),
        (1, 3, 0),
        (1, 4, 0),
        (2, 3, 0),
        (2, 4, 0),
        (3, 4, 0),
    ] {
        pattern_graph.add_arc(src, dst, elabel);
    }
    pattern_graph
}

fn create_mms() -> (Vec<MemoryManager>, Vec<MemoryManager>) {
    let count = 3;
    let (mut super_row_mms, mut index_mms) = (Vec::with_capacity(count), Vec::with_capacity(count));
    for _ in 0..count {
        super_row_mms.push(MemoryManager::Mem(vec![]));
        index_mms.push(MemoryManager::Mem(vec![]));
    }
    (super_row_mms, index_mms)
}

fn create_indices<'a>(
    super_row_mms: &'a [MemoryManager],
    index_mms: &'a [MemoryManager],
) -> Vec<Index<'a>> {
    super_row_mms
        .iter()
        .zip(index_mms)
        .map(|(super_row_mm, index_mm)| Index::new(super_row_mm, index_mm, &IndexType::Hash))
        .collect()
}

fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("q06")
        .arg(Arg::with_name("DATAGRAPH").required(true))
        .get_matches();
    let time_start = std::time::Instant::now();
    let data_graph = DataGraph::new(MemoryManager::MmapReadOnly(MmapReadOnlyFile::from_file(
        &File::open(matches.value_of("DATAGRAPH").unwrap())?,
    )));
    let pattern_graph = create_pattern();
    let (mut super_row_mms, mut index_mms) = create_mms();
    let time_now = std::time::Instant::now();
    match_characteristics(
        &data_graph,
        0,
        &[
            CharacteristicInfo::new(Characteristic::new(&pattern_graph, 1), 0),
            CharacteristicInfo::new(Characteristic::new(&pattern_graph, 2), 1),
            CharacteristicInfo::new(Characteristic::new(&pattern_graph, 3), 2),
        ],
        &mut super_row_mms,
        &mut index_mms,
    );
    eprintln!(
        "stars_time: {}",
        (std::time::Instant::now() - time_now).as_millis()
    );
    let time_now = std::time::Instant::now();
    let indices = create_indices(&super_row_mms, &index_mms);
    let mut num_srs: usize = 0;
    let mut num_rows: usize = 0;
    let mut row_set = HashSet::with_capacity(4);
    for sr0 in SuperRows::new(&super_row_mms[0]) {
        let u1 = sr0.images()[0][0];
        row_set.insert(u1);
        for &u2 in sr0.images()[1] {
            if row_set.insert(u2) {
                if let Some(sr1) = indices[1].get(u2) {
                    for &u3 in Intersection::new(vec![sr0.images()[1], sr1.images()[1]]) {
                        if row_set.insert(u3) {
                            if let Some(sr2) = indices[2].get(u3) {
                                num_srs += 1;
                                for &u4 in Intersection::new(vec![
                                    sr0.images()[1],
                                    sr1.images()[1],
                                    sr2.images()[1],
                                ]) {
                                    if row_set.insert(u4) {
                                        num_rows += 1;
                                        row_set.remove(&u4);
                                    }
                                }
                            }
                            row_set.remove(&u3);
                        }
                    }
                }
                row_set.remove(&u2);
            }
        }
        row_set.remove(&u1);
    }
    eprintln!(
        "join_time: {}",
        (std::time::Instant::now() - time_now).as_millis()
    );
    eprintln!("num_srs: {}", num_srs);
    eprintln!("num_rows: {}", num_rows);
    eprintln!(
        "total_time: {}",
        (std::time::Instant::now() - time_start).as_millis()
    );
    Ok(())
}
