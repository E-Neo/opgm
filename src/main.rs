use clap::{
    crate_authors, crate_description, crate_name, crate_version, App, AppSettings, Arg, ArgMatches,
    SubCommand,
};
use derive_more::Display;
use opgm::{
    compiler::compiler::compile,
    data_graph::{mm_read_sqlite3, DataGraph, DataGraphInfo},
    executor::{SuperRows, SuperRowsInfo},
    memory_manager::{MemoryManager, MmapFile, MmapReadOnlyFile},
    planner::{IndexType, MemoryManagerType, Task},
};
use rayon::prelude::*;
use std::{
    error::Error,
    fs::File,
    io::{BufWriter, Read, Write},
    path::PathBuf,
};

#[derive(Debug, Display, PartialEq)]
enum Err {
    CompileError(String),
}

impl std::error::Error for Err {}

fn handle_createdb(matches: &ArgMatches) -> Result<(), sqlite::Error> {
    let mut mm = MemoryManager::Mmap(MmapFile::new(matches.value_of("DATAGRAPH").unwrap()));
    mm_read_sqlite3(&mut mm, matches.value_of("SQLITE3").unwrap())
}

fn handle_dbinfo(matches: &ArgMatches) -> std::io::Result<()> {
    println!(
        "{}",
        DataGraphInfo::new(&DataGraph::new(MemoryManager::MmapReadOnly(
            MmapReadOnlyFile::from_file(&File::open(matches.value_of("DATAGRAPH").unwrap())?)
        )))
    );
    Ok(())
}

fn handle_displaydb(matches: &ArgMatches) -> std::io::Result<()> {
    println!(
        "{}",
        DataGraph::new(MemoryManager::MmapReadOnly(MmapReadOnlyFile::from_file(
            &File::open(matches.value_of("DATAGRAPH").unwrap())?
        )))
    );
    Ok(())
}

fn handle_displaysr(matches: &ArgMatches) -> std::io::Result<()> {
    for (i, sr) in SuperRows::new(&MemoryManager::MmapReadOnly(MmapReadOnlyFile::from_file(
        &File::open(matches.value_of("SRFILE").unwrap())?,
    )))
    .enumerate()
    {
        println!("{}: {}", i, sr);
    }
    Ok(())
}

fn handle_match(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let mut stderr = BufWriter::new(std::io::stderr());
    let start_time = std::time::Instant::now();
    let mut file = File::open(matches.value_of("DATAGRAPH").unwrap())?;
    let data_graph = DataGraph::new(if matches.is_present("db-in-memory") {
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        MemoryManager::Mem(buffer)
    } else {
        MemoryManager::MmapReadOnly(MmapReadOnlyFile::from_file(&file))
    });
    let query = std::fs::read_to_string(matches.value_of("QUERY").unwrap())?;
    let (pattern_graph, vc_info, ec_info, gcs) =
        compile(&query).map_err(|e| Err::CompileError(e.to_string()))?;
    let mut task = Task::new(&data_graph, pattern_graph, &vc_info, &ec_info, gcs);
    let plan = task
        .prepare()
        .star_sr_mm_type(parse_mm_type(
            matches.value_of("star-mm-type").unwrap(),
            matches.value_of("directory"),
            matches.value_of("name"),
        ))
        .index_mm_type(parse_mm_type(
            matches.value_of("index-mm-type").unwrap(),
            matches.value_of("directory"),
            matches.value_of("name"),
        ))
        .index_type(parse_index_type(matches.value_of("index-type").unwrap()))
        .plan();
    let (mut super_row_mms, mut index_mms) = plan.allocate();
    let mut time_now = std::time::Instant::now();
    plan.execute_stars_plan(&mut super_row_mms, &mut index_mms);
    writeln!(
        &mut stderr,
        "stars_time: {}",
        (std::time::Instant::now() - time_now).as_millis()
    )?;
    if matches.is_present("count-rows") {
        time_now = std::time::Instant::now();
        let num_rows = if plan.stars().is_empty() {
            0
        } else if let Some(srs) = plan.execute_join_plan(&super_row_mms, &index_mms) {
            if let Some(gc) = plan.global_constraint() {
                srs.par_bridge()
                    .map(|sr| {
                        sr.decompress(plan.join_plan().unwrap().sorted_vertex_eqv())
                            .filter(|row| gc.f()(row))
                            .count()
                    })
                    .sum()
            } else {
                srs.par_bridge()
                    .map(|sr| {
                        sr.decompress(plan.join_plan().unwrap().sorted_vertex_eqv())
                            .count()
                    })
                    .sum()
            }
        } else {
            todo!()
        };
        writeln!(
            &mut stderr,
            "join_decompress_time: {}",
            (std::time::Instant::now() - time_now).as_millis()
        )?;
        writeln!(&mut stderr, "num_rows: {}", num_rows)?;
    }
    writeln!(
        &mut stderr,
        "total_time: {}",
        (std::time::Instant::now() - start_time).as_millis()
    )?;
    Ok(())
}

fn handle_plan(matches: &ArgMatches) -> Result<(), Box<dyn Error>> {
    let data_graph = DataGraph::new(MemoryManager::MmapReadOnly(MmapReadOnlyFile::from_file(
        &File::open(matches.value_of("DATAGRAPH").unwrap())?,
    )));
    let query = std::fs::read_to_string(matches.value_of("QUERY").unwrap())?;
    let (pattern_graph, vc_info, ec_info, gcs) =
        compile(&query).map_err(|e| Err::CompileError(e.to_string()))?;
    println!(
        "{}",
        Task::new(&data_graph, pattern_graph, &vc_info, &ec_info, gcs)
            .prepare()
            .plan()
    );
    Ok(())
}

fn handle_srinfo(matches: &ArgMatches) -> std::io::Result<()> {
    let srfile = MemoryManager::MmapReadOnly(MmapReadOnlyFile::from_file(&File::open(
        matches.value_of("SRFILE").unwrap(),
    )?));
    println!("{}", SuperRowsInfo::new(&srfile));
    Ok(())
}

fn parse_mm_type<'a>(
    mm_type: &str,
    directory: Option<&str>,
    name: Option<&'a str>,
) -> MemoryManagerType<'a> {
    match mm_type {
        "mem" => MemoryManagerType::Mem,
        "mmap" => MemoryManagerType::Mmap(PathBuf::from(directory.unwrap()), name.unwrap()),
        "sink" => MemoryManagerType::Sink,
        _ => unreachable!(),
    }
}

fn parse_index_type(index_type: &str) -> IndexType {
    match index_type {
        "sorted" => IndexType::Sorted,
        "hash" => IndexType::Hash,
        _ => unreachable!(),
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new(crate_name!())
        .about(crate_description!())
        .author(crate_authors!())
        .version(crate_version!())
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            SubCommand::with_name("createdb")
                .about("Creates data graph database file")
                .after_help(
                    r"The SQLite3 file must contain the following schema:

  CREATE TABLE vertices (vid INT, vlabel INT);
  CREATE TABLE edges (src INT, dst INT, elabel INT);
",
                )
                .arg(Arg::with_name("SQLITE3").required(true))
                .arg(Arg::with_name("DATAGRAPH").required(true)),
        )
        .subcommand(
            SubCommand::with_name("dbinfo")
                .about("Displays information about the data graph")
                .arg(Arg::with_name("DATAGRAPH").required(true)),
        )
        .subcommand(
            SubCommand::with_name("displaydb")
                .about("Displays disk format of the data graph database file")
                .arg(Arg::with_name("DATAGRAPH").required(true)),
        )
        .subcommand(
            SubCommand::with_name("displaysr")
                .about("Displays SuperRow file")
                .arg(Arg::with_name("SRFILE").required(true)),
        )
        .subcommand(
            SubCommand::with_name("match")
                .about("Matches the query in the data graph")
                .arg(Arg::with_name("DATAGRAPH").required(true))
                .arg(Arg::with_name("QUERY").required(true))
                .arg(
                    Arg::with_name("count-rows")
                        .help("Counts rows of matching results")
                        .long("count-rows")
                        .takes_value(false)
                        .conflicts_with_all(&["to-stdout", "no-outfile"]),
                )
                .arg(
                    Arg::with_name("db-in-memory")
                        .help("Loads the data graph database into memory before matching")
                        .long("db-in-memory")
                        .takes_value(false),
                )
                .arg(
                    Arg::with_name("directory")
                        .help("Prefix memory mapped files with this directory")
                        .long("directory")
                        .takes_value(true)
                        .default_value("/tmp")
                        .required_ifs(&[
                            ("star-mm-type", "mmap"),
                            ("join-mm-type", "mmap"),
                            ("index-mm-type", "mmap"),
                        ]),
                )
                .arg(
                    Arg::with_name("name")
                        .help("Names prefix of the memory mapped files")
                        .long("name")
                        .takes_value(true)
                        .default_value("opgm")
                        .required_ifs(&[
                            ("star-mm-type", "mmap"),
                            ("join-mm-type", "mmap"),
                            ("index-mm-type", "mmap"),
                        ]),
                )
                .arg(
                    Arg::with_name("star-mm-type")
                        .help("The MemoryManager for star matching results")
                        .long("star-mm-type")
                        .takes_value(true)
                        .default_value("mmap")
                        .possible_values(&["mem", "mmap", "sink"]),
                )
                .arg(
                    Arg::with_name("index-mm-type")
                        .help("The MemoryManager for indices")
                        .long("index-mm-type")
                        .takes_value(true)
                        .default_value("mmap")
                        .possible_values(&["mem", "mmap", "sink"]),
                )
                .arg(
                    Arg::with_name("index-type")
                        .long("index-type")
                        .takes_value(true)
                        .default_value("sorted")
                        .possible_values(&["sorted", "hash"]),
                ),
        )
        .subcommand(
            SubCommand::with_name("plan")
                .about("Displays graph matching plan")
                .arg(Arg::with_name("DATAGRAPH").required(true))
                .arg(Arg::with_name("QUERY").required(true)),
        )
        .subcommand(
            SubCommand::with_name("srinfo")
                .about("Displays information about the SuperRow file")
                .arg(Arg::with_name("SRFILE").required(true)),
        )
        .get_matches();
    if let Some(matches) = matches.subcommand_matches("createdb") {
        handle_createdb(matches)?;
    } else if let Some(matches) = matches.subcommand_matches("dbinfo") {
        handle_dbinfo(matches)?;
    } else if let Some(matches) = matches.subcommand_matches("displaydb") {
        handle_displaydb(matches)?;
    } else if let Some(matches) = matches.subcommand_matches("displaysr") {
        handle_displaysr(matches)?;
    } else if let Some(matches) = matches.subcommand_matches("match") {
        handle_match(matches)?;
    } else if let Some(matches) = matches.subcommand_matches("plan") {
        handle_plan(matches)?;
    } else if let Some(matches) = matches.subcommand_matches("srinfo") {
        handle_srinfo(matches)?;
    }
    Ok(())
}
