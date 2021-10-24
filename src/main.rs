use clap::{crate_version, App, AppSettings, Arg, SubCommand};
use jerbs::{Command, Db, Time};
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::io::{self, Read, Write};
use std::os::unix::ffi::OsStringExt;
use tabled::{Style, Table, Tabled};

fn read_data() -> Vec<u8> {
    let mut buf = Vec::new();
    io::stdin().read_to_end(&mut buf).unwrap();
    buf
}

#[derive(PartialEq, Eq)]
enum BuildingHelp {
    No,
    Long,
    Short,
}

fn build_app(help: BuildingHelp) -> App<'static, 'static> {
    let mut app = App::new("jerbs")
        .version(crate_version!())
        .setting(AppSettings::DisableHelpSubcommand)
        .author("Kaz Wesley <jerbs@lambdaverse.org>")
        .about("Command-line work-stealing scheduler")
        .arg(
            Arg::with_name("help")
                .short("h")
                .long("help")
                .help("Print this help message documenting the most common subcommands")
                .long_help(
                    "Print a condensed help message documenting the most common subcommands",
                ),
        )
        .arg(
            Arg::with_name("long-help")
                .long("long-help")
                .help("Print an extended help message documenting all available subcommands")
                .long_help("Print this help message documenting all available subcommands"),
        )
        .arg(
            Arg::with_name("database")
                .help("Path to the jobs database file")
                .required_unless_one(&["help", "long-help"])
                .index(1),
        );
    let common_subcommands = vec![
        SubCommand::with_name("init").about("create a new jobs database"),
        SubCommand::with_name("create")
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
            )
            .arg(
                Arg::with_name("priority")
                    .help("the new job's priority (low = do sooner, default = 0)")
                    .short("p")
                    .long("priority")
                    .takes_value(true),
            ),
        SubCommand::with_name("list-available")
            .about("list jobs available to be taken")
            .arg(
                Arg::with_name("verbose")
                    .help("informative output for interactive use")
                    .short("v")
                    .long("verbose"),
            ),
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
        SubCommand::with_name("list-running")
            .about("list jobs logged as started and not finished")
            .arg(
                Arg::with_name("verbose")
                    .help("informative output for interactive use")
                    .short("v")
                    .long("verbose"),
            ),
        SubCommand::with_name("list-taken")
            .about("list jobs taken from the queue")
            .arg(
                Arg::with_name("verbose")
                    .help("informative output for interactive use")
                    .short("v")
                    .long("verbose"),
            ),
        SubCommand::with_name("monitor")
            .about("run a command, invoking log-start and log-finish appropriately")
            .arg(
                Arg::with_name("requeue")
                    .short("r")
                    .long("requeue-on-fail")
                    .help("If the command executes with non-zero status, put its job back in the queue"),
            )
            .arg(
                Arg::with_name("worker-id")
                    .help("any string identifying the worker taking the job")
                    .required(true)
                    .index(1),
            )
            .arg(
                Arg::with_name("command")
                    .help("command to run")
                    .required(true)
                    .multiple(true)
                    .last(true),
            ),
        SubCommand::with_name("modify")
            .about("alter an existing job")
            .arg(
                Arg::with_name("id")
                    .help("the id identifying the job")
                    .required(true)
                    .index(1),
            )
            .arg(
                Arg::with_name("add")
                    .help("the number of repetitions to add to the queue (can be negative)")
                    .short("a")
                    .long("add")
                    .takes_value(true),
            )
            .arg(
                Arg::with_name("priority")
                    .help("the job's new priority (low = do sooner, default = 0)")
                    .short("p")
                    .long("priority")
                    .takes_value(true),
            ),
    ];
    let uncommon_subcommands = vec![
        SubCommand::with_name("get-data")
            .about("get the data associated with a job")
            .arg(Arg::with_name("job-id").required(true).index(1)),
        SubCommand::with_name("get-count")
            .about("get the remaining count for a job")
            .arg(Arg::with_name("job-id").required(true).index(1)),
        SubCommand::with_name("log-start")
            .about("add a start event to the log")
            .arg(
                Arg::with_name("worker-id")
                    .help("any string identifying the worker taking the job")
                    .required(true)
                    .index(1),
            )
            .arg(
                Arg::with_name("command")
                    .help("command to log")
                    .multiple(true)
                    .last(true),
            ),
        SubCommand::with_name("log-finish")
            .about("add a finish event to the log")
            .arg(
                Arg::with_name("worker-id")
                    .help("any string identifying the worker taking the job")
                    .required(true)
                    .index(1),
            )
            .arg(
                Arg::with_name("result")
                    .help("integer result code (0 = no error)")
                    .required(true)
                    .index(2),
            ),
    ];
    app = app.subcommands(common_subcommands);
    if help != BuildingHelp::Short {
        app = app.subcommands(uncommon_subcommands);
    }
    app
}

#[derive(Tabled)]
struct Task {
    id: u32,
    count: u64,
    priority: i32,
    data: String,
}

#[derive(Tabled)]
struct JobStatus {
    worker: String,
    start_time: Paw<Time>,
    start_cmd: Paw<Command>,
    finish_result: Paw<i32>,
    finish_time: Paw<Time>,
    finish_data: Paw<MaybeUtf8>,
}

enum Paw<T> {
    Present(T),
    Absent,
    What,
}

impl<T> Display for Paw<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Paw::Present(x) => x.fmt(f),
            Paw::Absent => write!(f, ""),
            Paw::What => write!(f, "?"),
        }
    }
}

struct MaybeUtf8(Vec<u8>);

impl Display for MaybeUtf8 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(std::str::from_utf8(&self.0).unwrap_or("<data>"))
    }
}

fn print_statuses(jobs: impl IntoIterator<Item = jerbs::JobId>, db: &Db) -> jerbs::Result<()> {
    let mut entries = Vec::new();
    let mut worker_latest = HashMap::new();
    for job in jobs.into_iter() {
        let worker = db.get_job_worker(job)?;
        let latest = match worker_latest.get(&worker) {
            Some(latest) => *latest,
            None => {
                let latest = db.get_worker_latest_job(&worker)?.unwrap();
                worker_latest.insert(worker.to_string(), latest);
                latest
            }
        };
        let is_latest = job == latest;
        let start = db.get_job_start(job)?;
        let finish = db.get_job_finish(job)?;
        let start_time = start
            .as_ref()
            .map(|x| Paw::Present(x.time))
            .unwrap_or(if is_latest { Paw::Absent } else { Paw::What });
        let start_cmd = start.map(|x| Paw::Present(x.cmd)).unwrap_or(if is_latest {
            Paw::Absent
        } else {
            Paw::What
        });
        let finish_result = finish
            .as_ref()
            .map(|x| Paw::Present(x.result))
            .unwrap_or(if is_latest { Paw::Absent } else { Paw::What });
        let finish_time = finish
            .as_ref()
            .map(|x| Paw::Present(x.time))
            .unwrap_or(if is_latest { Paw::Absent } else { Paw::What });
        let finish_data = finish
            .map(|x| Paw::Present(MaybeUtf8(x.data)))
            .unwrap_or(if is_latest { Paw::Absent } else { Paw::What });
        entries.push(JobStatus {
            worker,
            start_time,
            start_cmd,
            finish_result,
            finish_time,
            finish_data,
        })
    }
    print!("{}", Table::new(entries).with(Style::pseudo_clean()));
    Ok(())
}

#[derive(Tabled)]
struct RunningStatus {
    worker: String,
    start_time: Time,
    start_cmd: Command,
}

fn print_running_statuses(
    jobs: impl IntoIterator<Item = jerbs::JobId>,
    db: &Db,
) -> jerbs::Result<()> {
    let mut entries = Vec::new();
    for job in jobs.into_iter() {
        let worker = db.get_job_worker(job)?;
        let start = db.get_job_start(job)?;
        let start_time = start.as_ref().unwrap().time;
        let start_cmd = start.unwrap().cmd;
        entries.push(RunningStatus {
            worker,
            start_time,
            start_cmd,
        })
    }
    print!("{}", Table::new(entries).with(Style::pseudo_clean()));
    Ok(())
}

fn main() -> jerbs::Result<()> {
    if std::env::args().len() < 2 {
        build_app(BuildingHelp::Short).print_help()?;
        std::process::exit(-1);
    }
    let mut app = build_app(BuildingHelp::No);
    let args = app
        .get_matches_from_safe_borrow(std::env::args())
        .unwrap_or_else(|e| e.exit());
    if args.is_present("long-help") {
        build_app(BuildingHelp::Long).print_long_help()?;
        std::process::exit(-1);
    } else if args.is_present("help") || !args.is_present("database") {
        build_app(BuildingHelp::Short).print_help()?;
        std::process::exit(-1);
    }

    let path = match args.value_of("database") {
        Some(path) => path,
        None => {
            build_app(BuildingHelp::Short).print_help()?;
            std::process::exit(-1);
        }
    };

    match args.subcommand() {
        ("init", Some(_)) => {
            let _ = Db::create(path)?;
        }
        ("create", Some(args)) => {
            let count = args
                .value_of("count")
                .unwrap()
                .parse()
                .expect("count must be integer");
            let priority = args
                .value_of("priority")
                .map(|x| x.parse().expect("priority must be integer"));
            let mut db = Db::open(path)?;
            let id = if let Some(data) = args.value_of("data") {
                db.new_job(data.as_bytes(), count, priority)?
            } else {
                let data = read_data();
                db.new_job(&data, count, priority)?
            };
            println!("{}", id);
        }
        ("modify", Some(args)) => {
            let task = args
                .value_of("id")
                .unwrap()
                .parse()
                .expect("job id must be integer");
            let add = args
                .value_of("add")
                .map(|x| x.parse().expect("add must be integer"));
            let prio = args
                .value_of("priority")
                .map(|x| x.parse().expect("priority must be integer"));
            let db = Db::open(path)?;
            if let Some(add) = add {
                db.add_count(task, add)?;
            }
            if let Some(prio) = prio {
                db.set_priority(task, prio)?;
            }
        }
        ("list-available", Some(args)) => {
            let verbose = args.is_present("verbose");
            let db = Db::open(path)?;
            let ids = db.job_ids_vec()?;
            if verbose {
                let mut entries = Vec::new();
                for id in ids {
                    let count = db.get_count(id)?;
                    let priority = db.get_priority(id)?;
                    let data = db.get_data(id)?;
                    let data = std::str::from_utf8(&data).unwrap_or("<data>");
                    entries.push(Task {
                        id,
                        count,
                        priority,
                        data: data.to_owned(),
                    });
                }
                print!("{}", Table::new(entries).with(Style::pseudo_clean()));
            } else {
                for id in ids {
                    println!("{}", id);
                }
            }
        }
        ("get-data", Some(args)) => {
            let id = args
                .value_of("job-id")
                .unwrap()
                .parse()
                .expect("job ids are integers");
            let data = Db::open(path)?.get_data(id)?;
            io::stdout().write_all(&data).unwrap();
        }
        ("get-count", Some(args)) => {
            let id = args
                .value_of("job-id")
                .unwrap()
                .parse()
                .expect("job ids are integers");
            let count = Db::open(path)?.get_count(id)?;
            println!("{}", count);
        }
        ("take", Some(args)) => {
            let mut db = Db::open(path)?;
            let worker = args.value_of("worker-id").unwrap();
            let wait = args.is_present("wait");
            if wait {
                todo!("take --wait")
            } else {
                let job = db.take(worker)?;
                if let Some(job) = job {
                    io::stdout().write_all(&job.data).unwrap();
                } else {
                    std::process::exit(2);
                }
            }
        }
        ("list-running", Some(args)) => {
            let verbose = args.is_present("verbose");
            let mut db = Db::open(path)?;
            if verbose {
                print_running_statuses(db.get_started_jobs()?, &db)?;
            } else {
                for job in db.get_started_jobs()? {
                    println!("{}", job);
                }
            }
        }
        ("list-taken", Some(args)) => {
            let verbose = args.is_present("verbose");
            let mut db = Db::open(path)?;
            if verbose {
                print_statuses(db.get_jobs()?, &db)?;
            } else {
                for job in db.get_jobs()? {
                    println!("{}", job);
                }
            }
        }
        ("log-start", Some(args)) => {
            let mut db = Db::open(path)?;
            let worker = args.value_of("worker-id").unwrap();
            let logcmd = args
                .values_of_os("command")
                .map(|args| args.map(|x| x.to_os_string().into_vec()).collect())
                .unwrap_or(vec![]);
            let id = db
                .current_job(worker)?
                .expect("worker currently has no job");
            db.log_start(id, logcmd)?;
        }
        ("log-finish", Some(args)) => {
            let mut db = Db::open(path)?;
            let worker = args.value_of("worker-id").unwrap();
            let result = args
                .value_of("result")
                .unwrap()
                .parse()
                .expect("result must be int");
            let id = db
                .current_job(worker)?
                .expect("worker currently has no job");
            db.log_finish(id, result)?;
        }
        ("monitor", Some(args)) => {
            use std::os::unix::process::ExitStatusExt;
            use std::process::Command;

            let requeue = args.is_present("requeue");
            let mut db = Db::open(path)?;
            let worker = args.value_of("worker-id").unwrap();
            let logcmd = args
                .values_of_os("command")
                .unwrap()
                .map(|x| x.to_os_string().into_vec())
                .collect();
            let id = db
                .current_job(worker)?
                .expect("worker currently has no job");
            db.log_start(id, logcmd)?;
            let mut cmd = args.values_of_os("command").unwrap();
            let exe = cmd.next().unwrap();
            let result = Command::new(exe).args(cmd).status();
            let log_code;
            let my_exit;
            match result {
                Ok(result) => {
                    // In the logs, we record signals as 256 + SIGNAL so it's always possible to
                    // distinguish them from regular exit codes.
                    log_code = result
                        .code()
                        .unwrap_or_else(|| 256 + result.signal().unwrap());
                    // In our return value, we report signals as 128 + SIGNAL (like bash), since we don't
                    // have enough return value space to keep signals distinct from exit codes.
                    my_exit = result
                        .code()
                        .unwrap_or_else(|| 128 + result.signal().unwrap());
                }
                Err(e) => {
                    eprintln!("Failed to start command: {}", e);
                    const EXIT_FAILED_TO_START: i32 = 512;
                    log_code = EXIT_FAILED_TO_START;
                    my_exit = -1;
                }
            }
            db.log_finish(id, log_code)?;
            if requeue && log_code != 0 {
                // TODO
            }
            std::process::exit(my_exit);
        }
        _ => build_app(BuildingHelp::Short).print_help()?,
    }
    Ok(())
}
