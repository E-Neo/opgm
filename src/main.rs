use clap::{load_yaml, App, AppSettings, ArgMatches};
use derive_more::Display;
use opgm::{
    compiler::compiler::compile,
    data_graph::{mm_read_sqlite3, DataGraph},
    executor::{SuperRows, SuperRowsInfo},
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
        println!(
            "{}: {:.2}% ({}/{}) {}",
            i,
            (sr.used_vertices() as f64) / (sr.allocated_vertices() as f64) * 100f64,
            sr.used_vertices(),
            sr.allocated_vertices(),
            sr
        );
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
        "StarsTime: {}",
        (std::time::Instant::now() - time_now).as_millis()
    )?;
    time_now = std::time::Instant::now();
    plan.execute_join(&mut super_row_mms, &mut index_mms);
    writeln!(
        std::io::stderr(),
        "JoinTime: {}",
        (std::time::Instant::now() - time_now).as_millis()
    )?;
    time_now = std::time::Instant::now();
    if !matches.is_present("no-outfile") {
        let num_rows = if matches.is_present("to-stdout") {
            plan.execute_write_results(&mut BufWriter::new(std::io::stdout()), &super_row_mms)?
        } else {
            plan.execute_write_results(
                &mut BufWriter::new(File::create(matches.value_of("OUTFILE").unwrap())?),
                &super_row_mms,
            )?
        };
        writeln!(
            std::io::stderr(),
            "DecompressTime: {}",
            (std::time::Instant::now() - time_now).as_millis()
        )?;
        writeln!(std::io::stderr(), "NumRows: {}", num_rows)?;
    }
    writeln!(
        std::io::stderr(),
        "TotalTime: {}",
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
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();
    if let Some(matches) = matches.subcommand_matches("createdb") {
        handle_createdb(matches)?;
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
