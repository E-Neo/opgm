use clap::{
    crate_authors, crate_description, crate_name, crate_version, App, AppSettings, Arg, ArgMatches,
    SubCommand,
};
use derive_more::Display;
use opgm::{
    compiler::compiler::compile,
    data_graph::{mm_read_sqlite3, DataGraph, DataGraphInfo},
    executor::{decompress, SuperRows, SuperRowsInfo},
    memory_manager::{MemoryManager, MmapFile, MmapReadOnlyFile},
    planner::{MemoryManagerType, Task},
};
use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;

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
        .join_sr_mm_type(parse_mm_type(
            matches.value_of("join-mm-type").unwrap(),
            matches.value_of("directory"),
            matches.value_of("name"),
        ))
        .index_mm_type(parse_mm_type(
            matches.value_of("index-mm-type").unwrap(),
            matches.value_of("directory"),
            matches.value_of("name"),
        ))
        .plan();
    let (mut super_row_mms, mut index_mms) = plan.allocate();
    let mut time_now = std::time::Instant::now();
    plan.execute_stars_matching(&mut super_row_mms, &mut index_mms);
    writeln!(
        std::io::stderr(),
        "stars_time: {}",
        (std::time::Instant::now() - time_now).as_millis()
    )?;
    time_now = std::time::Instant::now();
    plan.execute_join(&mut super_row_mms, &mut index_mms);
    writeln!(
        std::io::stderr(),
        "join_time: {}",
        (std::time::Instant::now() - time_now).as_millis()
    )?;
    time_now = std::time::Instant::now();
    if !matches.is_present("no-outfile") {
        let num_rows = if matches.is_present("count-rows") {
            if plan.stars().is_empty() {
                0
            } else {
                let vertex_eqv: Vec<_> = (if plan.join_plan().is_empty() {
                    plan.stars().last().unwrap().vertex_eqv()
                } else {
                    plan.join_plan().last().unwrap().vertex_eqv()
                })
                .iter()
                .map(|(&vertex, &eqv)| (vertex, eqv))
                .collect();
                decompress(
                    super_row_mms.last().unwrap(),
                    &vertex_eqv,
                    plan.global_constraint(),
                )
                .count()
            }
        } else if matches.is_present("to-stdout") {
            plan.execute_write_results(&mut BufWriter::new(std::io::stdout()), &super_row_mms)?
        } else {
            plan.execute_write_results(
                &mut BufWriter::new(File::create(matches.value_of("OUTFILE").unwrap())?),
                &super_row_mms,
            )?
        };
        writeln!(
            std::io::stderr(),
            "decompress_time: {}",
            (std::time::Instant::now() - time_now).as_millis()
        )?;
        writeln!(std::io::stderr(), "num_rows: {}", num_rows)?;
    }
    writeln!(
        std::io::stderr(),
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
        _ => panic!("Invalid mm-type"),
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
                .arg(Arg::with_name("OUTFILE").required_unless_one(&[
                    "count-rows",
                    "to-stdout",
                    "no-outfile",
                ]))
                .arg(
                    Arg::with_name("count-rows")
                        .help("Counts rows of matching results")
                        .long("count-rows")
                        .takes_value(false)
                        .conflicts_with_all(&["to-stdout", "no-outfile"]),
                )
                .arg(
                    Arg::with_name("to-stdout")
                        .help("Writes matching results to stdout")
                        .long("to-stdout")
                        .takes_value(false)
                        .conflicts_with_all(&["no-outfile"]),
                )
                .arg(
                    Arg::with_name("no-outfile")
                        .help("Disables decompression process")
                        .long("no-outfile")
                        .takes_value(false),
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
                    Arg::with_name("join-mm-type")
                        .help("The MemoryManager for join results")
                        .long("join-mm-type")
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
