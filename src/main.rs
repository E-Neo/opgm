use clap::{load_yaml, App, AppSettings, ArgMatches};
use derive_more::Display;
use opgm::{
    compiler::compiler::compile,
    data_graph::{mm_read_sqlite3, DataGraph},
    memory_manager::{MemoryManager, MmapFile, MmapReadOnlyFile},
    planner::Task,
};
use std::error::Error;
use std::fs::File;

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

fn main() -> Result<(), Box<dyn Error>> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();
    if let Some(matches) = matches.subcommand_matches("createdb") {
        handle_createdb(matches)?;
    } else if let Some(matches) = matches.subcommand_matches("displaydb") {
        handle_displaydb(matches)?;
    } else if let Some(matches) = matches.subcommand_matches("plan") {
        handle_plan(matches)?;
    }
    Ok(())
}
