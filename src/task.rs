use itertools::Itertools;

use crate::{
    data::{Graph, Index, Neighbor, NeighborIter, Vertex, VertexIter},
    executor::{
        count::{count_rows_join, count_rows_star},
        scan::vertex_centric,
    },
    front_end::{check, codegen, parse, rewrite},
    memory_manager::MemoryManager,
    planner::{decompose_stars, IndexType, JoinPlan, ScanPlan},
    types::VId,
};
use std::{marker::PhantomData, path::PathBuf, time::Instant};

pub enum MemoryManagerType {
    Mem,
    MmapMut(PathBuf, String), // (directory, name)
    Sink,
}

impl MemoryManagerType {
    fn new(mm_type: &str, directory: &str, name: &str) -> Self {
        match mm_type {
            "mem" => MemoryManagerType::Mem,
            "mmap" => MemoryManagerType::MmapMut(PathBuf::from(directory), String::from(name)),
            "sink" => MemoryManagerType::Sink,
            _ => unreachable!(),
        }
    }
}

pub enum ScanMethod {
    VertexCentric,
}

impl ScanMethod {
    fn new(scan_method: &str) -> Self {
        match scan_method {
            "vertex-centric" => ScanMethod::VertexCentric,
            _ => unreachable!(),
        }
    }
}

pub enum Job {
    CountRows,
}

impl Job {
    fn new(job: &str) -> Self {
        match job {
            "count-rows" => Job::CountRows,
            _ => unreachable!(),
        }
    }
}

pub struct Task<G, GIdx, LIdx, VIter, NIter, V, N>
where
    G: Graph<GIdx>,
    GIdx: Index<VIter>,
    LIdx: Index<NIter>,
    VIter: VertexIter<V>,
    NIter: NeighborIter<N>,
    V: Vertex<LIdx>,
    N: Neighbor,
{
    data: G,
    query: String,
    sr_mm_type: MemoryManagerType,
    index_mm_type: MemoryManagerType,
    index_type: IndexType,
    roots: Option<Vec<VId>>,
    scan_method: ScanMethod,
    job: Job,
    _phantom_gidx: PhantomData<GIdx>,
    _phantom_lidx: PhantomData<LIdx>,
    _phantom_viter: PhantomData<VIter>,
    _phantom_niter: PhantomData<NIter>,
    _phantom_v: PhantomData<V>,
    _phantom_n: PhantomData<N>,
}

impl<G, GIdx, LIdx, VIter, NIter, V, N> Task<G, GIdx, LIdx, VIter, NIter, V, N>
where
    G: Graph<GIdx>,
    GIdx: Index<VIter>,
    LIdx: Index<NIter>,
    VIter: VertexIter<V>,
    NIter: NeighborIter<N>,
    V: Vertex<LIdx>,
    N: Neighbor,
{
    pub fn new(
        data: G,
        query: String,
        directory: &str,
        name: &str,
        sr_mm_type: &str,
        index_mm_type: &str,
        index_type: &str,
        roots: Option<Vec<VId>>,
        scan_method: &str,
        job: &str,
    ) -> Self {
        Self {
            data,
            query: String::from(query),
            sr_mm_type: MemoryManagerType::new(sr_mm_type, directory, name),
            index_mm_type: MemoryManagerType::new(index_mm_type, directory, name),
            index_type: match index_type {
                "hash" => IndexType::Hash,
                "sorted" => IndexType::Sorted,
                _ => unreachable!(),
            },
            roots,
            scan_method: ScanMethod::new(scan_method),
            job: Job::new(job),
            _phantom_gidx: PhantomData,
            _phantom_lidx: PhantomData,
            _phantom_viter: PhantomData,
            _phantom_niter: PhantomData,
            _phantom_v: PhantomData,
            _phantom_n: PhantomData,
        }
    }

    pub fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        let ast = parse(&self.query)?;
        check(&ast)?;
        let (pattern, _gcs) = codegen(
            ast.vertices(),
            ast.arcs(),
            ast.edges(),
            ast.constraint()
                .map_or(vec![], |expr| rewrite(expr.clone())),
        );
        let roots = self.roots.unwrap_or(decompose_stars(&self.data, &pattern));
        let scan_plan = ScanPlan::new(&pattern, &roots);
        eprintln!(
            "root_ids: [{}]",
            scan_plan
                .stars()
                .iter()
                .map(|star| format!("(u{}, {})", star.root(), star.id()))
                .join(", ")
        );
        let mut sr_mms = create_super_row_mms(&self.sr_mm_type, roots.len());
        let mut index_mms = create_index_mms(&self.index_mm_type, roots.len());
        match self.scan_method {
            ScanMethod::VertexCentric => {
                for (vlabel, infos) in scan_plan.plan() {
                    let time_now = Instant::now();
                    vertex_centric::match_characteristics(
                        &self.data,
                        *vlabel,
                        infos,
                        &mut sr_mms,
                        &mut index_mms,
                    );
                    eprintln!(
                        "scan_time({}): {}",
                        vlabel,
                        (Instant::now() - time_now).as_millis()
                    );
                }
            }
        }
        let time_now = Instant::now();
        match self.job {
            Job::CountRows => {
                let num_rows = match sr_mms.as_slice() {
                    [] => 0,
                    [sr_mm] => {
                        let mut vertex_eqv: Vec<_> = scan_plan.stars()[0]
                            .vertex_eqv()
                            .iter()
                            .map(|(&vid, &eqv)| (vid, eqv))
                            .collect();
                        vertex_eqv.sort();
                        count_rows_star(sr_mm, &vertex_eqv)
                    }
                    _ => {
                        let join_plan = JoinPlan::new(&pattern, self.index_type, scan_plan.stars());
                        eprintln!("indexed_joins: {:?}", join_plan.indexed_joins());
                        eprintln!("intersections: {:?}", join_plan.intersections());
                        count_rows_join(&sr_mms, &index_mms, &join_plan)
                    }
                };
                eprintln!("num_rows: {}", num_rows);
            }
        }
        eprintln!("job_time: {}", (Instant::now() - time_now).as_millis());
        eprintln!("total_time: {}", (Instant::now() - start_time).as_millis());
        Ok(())
    }
}

fn create_super_row_mms(mm_type: &MemoryManagerType, count: usize) -> Vec<MemoryManager> {
    (0..count)
        .map(|id| match mm_type {
            MemoryManagerType::Mem => MemoryManager::new_mem(0),
            MemoryManagerType::MmapMut(path, name) => {
                MemoryManager::new_mmap_mut(path.join(format!("{}{}.sr", name, id)), 0).unwrap()
            }
            MemoryManagerType::Sink => MemoryManager::Sink,
        })
        .collect()
}

fn create_index_mms(mm_type: &MemoryManagerType, count: usize) -> Vec<MemoryManager> {
    (0..count)
        .map(|id| match mm_type {
            MemoryManagerType::Mem => MemoryManager::new_mem(0),
            MemoryManagerType::MmapMut(path, name) => {
                MemoryManager::new_mmap_mut(path.join(format!("{}{}.idx", name, id)), 0).unwrap()
            }
            MemoryManagerType::Sink => MemoryManager::Sink,
        })
        .collect()
}
