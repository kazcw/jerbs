mod db;

use clap::{App, AppSettings, Arg, SubCommand};
use db::Db;
use std::io::{self, Read, Write};

fn read_data() -> Vec<u8> {
    let mut buf = Vec::new();
    io::stdin().read_to_end(&mut buf).unwrap();
    buf
}

fn main() -> db::Result<()> {
    let args = App::new("jerbs")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .version("0.1")
        .author("Kaz Wesley <jerbs@lambdaverse.org>")
        .about("Command-line work-stealing scheduler")
        .arg(
            Arg::with_name("database")
                .help("Path to the jobs database file")
                .required(true)
                .index(1),
        )
        .subcommand(SubCommand::with_name("new").about("create a new jobs database"))
        .subcommand(
            SubCommand::with_name("new-job")
                .about("define a job")
                .arg(
                    Arg::with_name("count")
                        .help("the number of repetitions to enqueue initially")
                        .short("c")
                        .long("count")
                        .takes_value(true)
                        .default_value("0"),
                )
                .arg(
                    Arg::with_name("data")
                        .help("the data associated with the job")
                        .short("d")
                        .long("data")
                        .takes_value(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("list-jobs")
                .about("show all defined jobs")
                .arg(
                    Arg::with_name("verbose")
                        .help("informative output for interactive use")
                        .short("v")
                        .long("verbose"),
                ),
        )
        .subcommand(
            SubCommand::with_name("get-data")
                .about("get the data associated with a job")
                .arg(Arg::with_name("job-id").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("get-count")
                .about("get the remaining count for a job")
                .arg(Arg::with_name("job-id").required(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("take")
                .about("take a job from the queue")
                .arg(
                    Arg::with_name("wait")
                        .help("wait for a job to become available")
                        .short("w")
                        .long("wait"),
                )
                .arg(
                    Arg::with_name("worker-id")
                        .help("any string identifying the worker taking the job")
                        .required(true)
                        .index(1),
                ),
        )
        .get_matches();

    let path = args.value_of("database").unwrap();

    match args.subcommand() {
        ("new", Some(_)) => {
            let _ = Db::create(path)?;
            Ok(())
        }
        ("new-job", Some(args)) => {
            let count = args
                .value_of("count")
                .unwrap()
                .parse()
                .expect("count must be integer");
            let mut db = Db::open(path)?;
            let id = if let Some(data) = args.value_of("data") {
                db.new_job(data.as_bytes(), count)?
            } else {
                let data = read_data();
                db.new_job(&data, count)?
            };
            println!("{}", id);
            Ok(())
        }
        ("list-jobs", Some(args)) => {
            let verbose = args.is_present("verbose");
            let db = Db::open(path)?;
            let ids = db.job_ids_vec()?;
            if verbose {
                for id in ids {
                    let count = db.get_count(id)?;
                    let data = db.get_data(id)?;
                    let data = std::str::from_utf8(&data).unwrap_or("<data>");
                    println!("{}\t{}\t{}", id, count, data);
                }
            } else {
                for id in ids {
                    println!("{}", id);
                }
            }
            Ok(())
        }
        ("get-data", Some(args)) => {
            let id = args
                .value_of("job-id")
                .unwrap()
                .parse()
                .expect("job ids are integers");
            let data = Db::open(path)?.get_data(id)?;
            io::stdout().write_all(&data).unwrap();
            Ok(())
        }
        ("get-count", Some(args)) => {
            let id = args
                .value_of("job-id")
                .unwrap()
                .parse()
                .expect("job ids are integers");
            let count = Db::open(path)?.get_count(id)?;
            println!("{}", count);
            Ok(())
        }
        ("take", Some(args)) => {
            let mut db = Db::open(path)?;
            let worker = args.value_of("worker-id").unwrap();
            let wait = args.is_present("wait");
            if wait {
                todo!("take --wait")
            } else {
                let data = db.take(worker)?;
                if let Some(data) = data {
                    io::stdout().write_all(&data).unwrap();
                    Ok(())
                } else {
                    std::process::exit(2);
                }
            }
        }
        _ => unreachable!(),
    }
}
