use clap::{clap_app, crate_authors, crate_description, crate_name, crate_version, ArgMatches};
use opgm::{
    constants::{MAGIC_MULTIPLE, MAGIC_SINGLE},
    data::{info_edges, multiple, single, Graph},
    memory_manager::MemoryManager,
    task::Task,
    types::VId,
};

fn handle_createdb(matches: &ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    let mut mm = MemoryManager::new_mmap_mut(matches.value_of("PATH").unwrap(), 0)?;
    let info_edges_mm = MemoryManager::new_mmap(matches.value_of("DATA").unwrap())?;
    match matches.value_of("FMT").unwrap() {
        "multiple" => multiple::create::mm_from_info_edges(&mut mm, &info_edges_mm),
        "single" => single::create::mm_from_info_edges(&mut mm, &info_edges_mm),
        _ => unreachable!(),
    }
    Ok(())
}

fn handle_dbinfo(matches: &ArgMatches) -> std::io::Result<()> {
    let data_mm = MemoryManager::new_mmap(matches.value_of("DATAGRAPH").unwrap())?;
    let info = match unsafe { *data_mm.as_ref::<u64>(0) } {
        MAGIC_MULTIPLE => multiple::DataGraph::new(&data_mm).info(),
        MAGIC_SINGLE => single::DataGraph::new(&&data_mm).info(),
        _ => unreachable!(),
    };
    println!("{}", info);
    Ok(())
}

fn handle_run(matches: &ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    let data_mm = MemoryManager::new_mmap(matches.value_of("DATAGRAPH").unwrap())?;
    let query = if let Some(query_path) = matches.value_of("QUERY_PATH") {
        std::fs::read_to_string(query_path)?
    } else {
        String::from(matches.value_of("QUERY").unwrap())
    };
    let directory = matches.value_of("DIRECTORY").unwrap();
    let name = matches.value_of("NAME").unwrap();
    let sr_mm_type = matches.value_of("SR-MM-TYPE").unwrap();
    let index_mm_type = matches.value_of("INDEX-MM-TYPE").unwrap();
    let index_type = matches.value_of("INDEX-TYPE").unwrap();
    let roots = matches
        .values_of("ROOTS")
        .map(|roots| roots.map(|root| root.parse::<VId>().unwrap()).collect());
    let scan_method = matches.value_of("SCAN-METHOD").unwrap();
    let join_method = matches.value_of("JOIN-METHOD").unwrap();
    match unsafe { *data_mm.as_ref::<u64>(0) } {
        MAGIC_MULTIPLE => Task::new(
            multiple::DataGraph::new(&data_mm),
            query,
            directory,
            name,
            sr_mm_type,
            index_mm_type,
            index_type,
            roots,
            scan_method,
            join_method,
        )
        .run(),
        MAGIC_SINGLE => Task::new(
            single::DataGraph::new(&data_mm),
            query,
            directory,
            name,
            sr_mm_type,
            index_mm_type,
            index_type,
            roots,
            scan_method,
            join_method,
        )
        .run(),
        _ => panic!("unrecognized data graph format"),
    }
}

fn handle_sort(matches: &ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    info_edges::mm_from_sqlite(
        &mut MemoryManager::new_mmap_mut(matches.value_of("PATH").unwrap(), 0)?,
        &rusqlite::Connection::open(matches.value_of("SQLITE").unwrap())?,
    )?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = std::env::temp_dir();
    #[rustfmt::skip]
    let matches = clap_app!((crate_name!()) =>
        (version: crate_version!())
        (author: crate_authors!())
        (about: crate_description!())
        (setting: clap::AppSettings::SubcommandRequiredElseHelp)
        (@subcommand createdb =>
            (about: "Creates data graph file")
            (@arg FMT: * possible_value[multiple single])
            (@arg DATA: *)
            (@arg PATH: *)
        )
        (@subcommand dbinfo =>
            (about: "Displays information about the data graph")
            (@arg DATAGRAPH: *)
        )
        (@subcommand run =>
            (about: "Evaluate the graph matching query")
            (@arg DATAGRAPH: *)
            (@group INPUT +required =>
                (@arg QUERY_PATH:)
                (@arg QUERY: -e --execute +takes_value)
            )
            (@arg DIRECTORY: --directory +takes_value
                  default_value(temp_dir.to_str().ok_or("temp dir")?))
            (@arg NAME: --name +takes_value default_value[opgm])
            (@arg ("SR-MM-TYPE"): --("sr-mm-type") +takes_value default_value[mmap]
                  possible_value[mem mmap sink])
            (@arg ("INDEX-MM-TYPE"): --("index-mm-type") +takes_value default_value[mmap]
                  possible_value[mem mmap sink])
            (@arg ("INDEX-TYPE"): --("index-type") +takes_value default_value[hash]
                  possible_value[sorted hash])
            (@arg ROOTS: --roots +takes_value +require_delimiter ...
                  { |n| {
                      n.parse::<VId>()
                          .map_or_else(|_| Err(String::from("invalid root")), |_| Ok(()))} }
            )
            (@arg ("SCAN-METHOD"): --("scan-method") +takes_value default_value("vertex-centric")
                  possible_value("vertex-centric"))
            (@arg ("JOIN-METHOD"): --("join-method") +takes_value default_value("count-rows")
                  possible_values(&["count-rows", "count-rows-slow"]))
        )
        (@subcommand sort =>
            (about: "Sort the SQLite graph data")
            (after_help: r"The SQLite3 file must contain the following schema:

  CREATE TABLE vertices (vid INT, vlabel INT);
  CREATE TABLE edges (src INT, dst INT, elabel INT);
")
            (@arg SQLITE: *)
            (@arg PATH: *)
        )
    ).get_matches();
    if let Some(matches) = matches.subcommand_matches("createdb") {
        handle_createdb(matches)?;
    } else if let Some(matches) = matches.subcommand_matches("dbinfo") {
        handle_dbinfo(matches)?;
    } else if let Some(matches) = matches.subcommand_matches("run") {
        handle_run(matches)?;
    } else if let Some(matches) = matches.subcommand_matches("sort") {
        handle_sort(matches)?;
    }
    Ok(())
}
