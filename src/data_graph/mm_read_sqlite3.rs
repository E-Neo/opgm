use crate::{
    data_graph::mm_read_iter,
    memory_manager::MemoryManager,
    types::{ELabel, VId, VLabel},
};
use std::path::Path;

fn query_usize(conn: &sqlite::Connection, query: &str) -> usize {
    let mut stmt = conn.prepare(query).unwrap();
    stmt.next().unwrap();
    stmt.read::<i64>(0).unwrap() as usize
}

fn query_num_vlabels(conn: &sqlite::Connection) -> usize {
    query_usize(conn, "SELECT COUNT(DISTINCT vlabel) FROM vertices")
}

fn query_num_vertices(conn: &sqlite::Connection) -> usize {
    query_usize(conn, "SELECT COUNT(*) FROM vertices")
}

fn query_num_edges(conn: &sqlite::Connection) -> usize {
    query_usize(conn, "SELECT COUNT(*) FROM edges")
}

struct VertexIter<'a> {
    stmt: sqlite::Statement<'a>,
}

impl<'a> VertexIter<'a> {
    fn new(conn: &sqlite::Connection) -> VertexIter {
        let stmt = conn.prepare("SELECT * FROM vertices").unwrap();
        VertexIter { stmt }
    }
}

impl<'a> Iterator for VertexIter<'a> {
    type Item = (VId, VLabel);

    fn next(&mut self) -> Option<Self::Item> {
        if let sqlite::State::Row = self.stmt.next().unwrap() {
            Some((
                self.stmt.read::<i64>(0).unwrap() as VId,
                self.stmt.read::<i64>(1).unwrap() as VLabel,
            ))
        } else {
            None
        }
    }
}

struct EdgeIter<'a> {
    stmt: sqlite::Statement<'a>,
}

impl<'a> EdgeIter<'a> {
    fn new(conn: &sqlite::Connection) -> EdgeIter {
        let stmt = conn.prepare("SELECT * FROM edges").unwrap();
        EdgeIter { stmt }
    }
}

impl<'a> Iterator for EdgeIter<'a> {
    type Item = (VId, VId, ELabel);

    fn next(&mut self) -> Option<Self::Item> {
        if let sqlite::State::Row = self.stmt.next().unwrap() {
            Some((
                self.stmt.read::<i64>(0).unwrap() as VId,
                self.stmt.read::<i64>(1).unwrap() as VId,
                self.stmt.read::<i64>(2).unwrap() as ELabel,
            ))
        } else {
            None
        }
    }
}

fn query_vertices(conn: &sqlite::Connection) -> VertexIter {
    VertexIter::new(conn)
}

fn query_edges(conn: &sqlite::Connection) -> EdgeIter {
    EdgeIter::new(conn)
}

fn mm_read_sqlite3_connection(mm: &mut MemoryManager, conn: &sqlite::Connection) {
    mm_read_iter(
        mm,
        query_num_vlabels(&conn),
        query_num_vertices(&conn),
        query_num_edges(&conn),
        query_vertices(&conn),
        query_edges(&conn),
    );
}

/// Reads the data graph stored in the SQLite3 file `path` into `mm`.
///
/// The SQLite3 file must have the following schema:
///
/// ```sql
/// CREATE TABLE vertices (vid INT, vlabel INT);
/// CREATE TABLE edges (src INT, dst INT, elabel INT);
/// ```
pub fn mm_read_sqlite3<P: AsRef<Path>>(mm: &mut MemoryManager, path: P) -> sqlite::Result<()> {
    let conn = sqlite::open(path)?;
    conn.execute("BEGIN;")?;
    mm_read_sqlite3_connection(mm, &conn);
    conn.execute("END;")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_triangle_sqlite3() -> sqlite::Connection {
        let conn = sqlite::Connection::open(":memory:").unwrap();
        conn.execute("CREATE TABLE vertices (vid INT, vlabel INT);")
            .unwrap();
        conn.execute("CREATE TABLE edges (src INT, dst INT, elabel INT);")
            .unwrap();
        conn.execute("INSERT INTO vertices VALUES (1, 10), (2, 20), (3, 20);")
            .unwrap();
        conn.execute("INSERT INTO edges VALUES (1, 2, 12), (1, 3, 13), (2, 3, 23), (3, 2, 32);")
            .unwrap();
        conn
    }

    fn create_triange_mm() -> MemoryManager {
        let mut mm = MemoryManager::Mem(vec![]);
        mm_read_iter(
            &mut mm,
            2,
            3,
            4,
            vec![(1, 10), (2, 20), (3, 20)].into_iter(),
            vec![(1, 2, 12), (1, 3, 13), (2, 3, 23), (3, 2, 32)].into_iter(),
        );
        mm
    }

    #[test]
    fn test_mm_read_sqlite3() {
        let mut sqlite3_mm = MemoryManager::Mem(vec![]);
        mm_read_sqlite3_connection(&mut sqlite3_mm, &create_triangle_sqlite3());
        let mm = create_triange_mm();
        assert_eq!(
            sqlite3_mm.read_slice::<u8>(0, sqlite3_mm.len()),
            mm.read_slice::<u8>(0, mm.len())
        );
    }
}
