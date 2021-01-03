use opgm::{
    compiler::compiler::compile,
    data_graph::{mm_read_iter, DataGraph},
    memory_manager::MemoryManager,
    planner::Task,
};
use std::collections::HashSet;

const QUERY: &str = "\
(match (vertices (u1 1) (u2 2) (u3 3) (u4 4))
       (arcs (u1 u2 0) (u2 u3 0) (u3 u4 0)))";

fn create_data_graph() -> DataGraph {
    let vertices = vec![(1, 1), (2, 2), (3, 3), (4, 4), (11, 1), (14, 4)];
    let arcs = vec![(1, 2, 0), (2, 3, 0), (3, 4, 0), (3, 14, 0), (11, 2, 0)];
    let mut mm = MemoryManager::Mem(vec![]);
    mm_read_iter(
        &mut mm,
        vertices
            .iter()
            .map(|&(_, l)| l)
            .collect::<HashSet<_>>()
            .len(),
        vertices.len(),
        arcs.len(),
        vertices,
        arcs,
    );
    DataGraph::new(mm)
}

#[test]
fn test_match() {
    let data_graph = create_data_graph();
    let (pattern_graph, vc_info, ec_info, gcs) = compile(QUERY).unwrap();
    let mut task = Task::new(&data_graph, pattern_graph, &vc_info, &ec_info, gcs);
    let plan = task.prepare().plan();
    assert_eq!(
        plan.stars()
            .iter()
            .map(|star| star.root())
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    let (mut super_row_mms, mut index_mms) = plan.allocate();
    assert_eq!(super_row_mms.len(), 2);
    assert_eq!(index_mms.len(), 2);
    plan.execute_stars_plan(&mut super_row_mms, &mut index_mms);
    let result: Vec<_> = plan
        .execute_join_plan(&super_row_mms, &mut index_mms)
        .unwrap()
        .flat_map(|sr| sr.decompress(plan.join_plan().unwrap().sorted_vertex_eqv()))
        .collect();
    assert_eq!(
        result,
        vec![
            vec![2, 3, 1, 4],
            vec![2, 3, 1, 14],
            vec![2, 3, 11, 4],
            vec![2, 3, 11, 14]
        ]
    );
}
