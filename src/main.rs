use clap::{clap_app, crate_authors, crate_description, crate_name, crate_version, ArgMatches};
use opgm::{
    data_graph::{self, Graph},
    memory_manager::MemoryManager,
};

fn handle_createdb(matches: &ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    match matches.value_of("FMT").unwrap() {
        "multiple" => {
            data_graph::multiple::create::mm_from_sqlite(
                &mut MemoryManager::new_mmap_mut(matches.value_of("PATH").unwrap(), 0)?,
                &rusqlite::Connection::open(matches.value_of("SQLITE").unwrap())?,
            )?;
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn handle_dbinfo(matches: &ArgMatches) -> std::io::Result<()> {
    let data_graph_mm = MemoryManager::new_mmap(matches.value_of("DATAGRAPH").unwrap())?;
    let info = match matches.value_of("FMT").unwrap() {
        "multiple" => data_graph::multiple::DataGraph::new(&data_graph_mm).info(),
        _ => unreachable!(),
    };
    println!("{}", info);
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[rustfmt::skip]
    let matches = clap_app!((crate_name!()) =>
        (version: crate_version!())
        (author: crate_authors!())
        (about: crate_description!())
        (setting: clap::AppSettings::SubcommandRequiredElseHelp)
        (@subcommand createdb =>
            (about: "Creates data graph file")
            (after_help: r"The SQLite3 file must contain the following schema:

  CREATE TABLE vertices (vid INT, vlabel INT);
  CREATE TABLE edges (src INT, dst INT, elabel INT);
")
            (@arg FMT: +required possible_value[multiple])
            (@arg SQLITE: +required)
            (@arg PATH: +required))
        (@subcommand dbinfo =>
            (about: "Displays information about the data graph")
            (@arg FMT: +required possible_value[multiple])
            (@arg DATAGRAPH: +required))
    ).get_matches();
    if let Some(matches) = matches.subcommand_matches("createdb") {
        handle_createdb(matches)?;
    } else if let Some(matches) = matches.subcommand_matches("dbinfo") {
        handle_dbinfo(matches)?;
    }
    Ok(())
}
