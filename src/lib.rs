use rusqlite::params;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

const DB_VERSION: u32 = 2;

pub type JobId = u32;
pub type TaskId = u32;

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    DbTooNew { db_version: u32 },
}
impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::DbTooNew { db_version } =>
                write!(f,
                       "Database schema is from a newer version of jerbs! Version found: {}. Max version supported: {}.",
                       db_version,
                       DB_VERSION)
        }
    }
}
impl std::error::Error for Error {}
pub type Result<T> = anyhow::Result<T>;

#[derive(PartialEq, Eq, Debug)]
pub struct Job {
    pub id: JobId,
    pub data: Vec<u8>,
}

pub struct Db {
    conn: Connection,
}

fn prepare_conn(conn: &Connection) -> Result<()> {
    conn.execute("PRAGMA foreign_keys = 1", [])?;
    Ok(())
}

fn get_version(conn: &Connection) -> Result<u32> {
    let mut version = conn.prepare("SELECT version FROM meta")?;
    let mut version = version.query([])?;
    let version = version.next()?.unwrap().get(0)?;
    Ok(version)
}

fn pre_upgrade(conn: &Connection, v0: u32, v1: u32) -> Result<()> {
    eprintln!("upgrading database: version {} -> version {}", v0, v1);
    conn.execute("PRAGMA foreign_keys = 0", [])?;
    Ok(())
}

fn post_upgrade(conn: &Connection) -> Result<()> {
    conn.execute("PRAGMA foreign_key_check", [])?;
    conn.execute("PRAGMA foreign_keys = 1", [])?;
    Ok(())
}

fn upgrade_v1(conn: &Connection) -> Result<()> {
    pre_upgrade(conn, 1, 2)?;

    conn.execute("ALTER TABLE job RENAME TO task", [])?;
    conn.execute("ALTER TABLE task ADD priority INTEGER", [])?;

    conn.execute(
        "CREATE TABLE job (id INTEGER PRIMARY KEY, task REFERENCES task, worker TEXT NOT NULL)",
        [],
    )?;
    conn.execute(
        "INSERT INTO job (id, task, worker) SELECT id, job, data FROM worker",
        [],
    )?;
    conn.execute("DROP TABLE worker", [])?;

    conn.execute(
        "CREATE TABLE job_start (job PRIMARY KEY REFERENCES job, time INTEGER, cmd BLOB)",
        [],
    )?;
    conn.execute("CREATE TABLE job_finish (job PRIMARY KEY REFERENCES job, result INTEGER, time INTEGER, data BLOB)", [])?;
    conn.execute("UPDATE meta SET version = ?", [DB_VERSION])?;

    post_upgrade(conn)
}

fn upgrade(conn: &mut Connection) -> Result<()> {
    loop {
        let tx = conn.transaction()?;
        let version = get_version(&tx)?;
        match version {
            1 => upgrade_v1(&tx)?,
            DB_VERSION => break Ok(()),
            db_version => break Err(Error::DbTooNew { db_version }.into()),
        }
        tx.commit()?;
    }
}

impl Db {
    pub fn create(path: &str) -> Result<Self> {
        // TODO: fail right away if the path exists--would give a clearer error message than
        // bailing on a CREATE TABLE below.
        let conn = Connection::open(path)?;

        Self::create_from_conn(conn)
    }

    fn create_from_conn(conn: Connection) -> Result<Self> {
        prepare_conn(&conn)?;

        conn.execute("CREATE TABLE meta (version INTEGER)", [])?;
        conn.execute("CREATE TABLE task (id INTEGER PRIMARY KEY, count INTEGER NOT NULL, data BLOB NOT NULL, priority INTEGER)", [])?;
        conn.execute("CREATE TABLE job (id INTEGER PRIMARY KEY, task REFERENCES task, time INTEGER, worker TEXT NOT NULL)", [])?;
        conn.execute(
            "CREATE TABLE job_start (job PRIMARY KEY REFERENCES job, time INTEGER, cmd BLOB)",
            [],
        )?;
        conn.execute("CREATE TABLE job_finish (job PRIMARY KEY REFERENCES job, result INTEGER, time INTEGER, data BLOB)", [])?;
        conn.execute("INSERT INTO meta VALUES (?)", [DB_VERSION])?;

        Ok(Self { conn })
    }

    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        Self::open_from_conn(conn)
    }

    fn open_from_conn(mut conn: Connection) -> Result<Self> {
        prepare_conn(&conn)?;
        upgrade(&mut conn)?;
        Ok(Self { conn })
    }

    pub fn take(&mut self, worker: &str) -> Result<Option<Job>> {
        const JOB_Q: &str = "SELECT task.id, task.data FROM task \
           LEFT JOIN (SELECT job.task, count(1) as c FROM job GROUP BY job.task) as w
           ON w.task = task.id \
         WHERE COALESCE(w.c, 0) < task.count \
         ORDER BY COALESCE(task.priority, 0), task.id LIMIT 1";
        let job;
        let tx = self.conn.transaction()?;
        {
            let mut job_q = tx.prepare(JOB_Q)?;
            let mut jobs = job_q.query([])?;
            let row = match jobs.next()? {
                Some(row) => row,
                None => return Ok(None),
            };
            job = Job {
                id: row.get(0)?,
                data: row.get(1)?,
            };
            tx.execute(
                "INSERT INTO job (task, worker) VALUES (?, ?)",
                params![job.id, worker],
            )?;
        }
        tx.commit()?;

        Ok(Some(job))
    }

    pub fn new_job(&mut self, data: &[u8], count: u64, priority: Option<i32>) -> Result<u32> {
        self.conn.execute(
            "INSERT INTO task (data, count, priority) VALUES (?, ?, ?)",
            params![data, count, priority],
        )?;
        let id = self.conn.last_insert_rowid() as TaskId;

        Ok(id)
    }

    // TODO: iterator version. Has to own its Statement.
    pub fn job_ids_vec(&self) -> Result<Vec<TaskId>> {
        let mut q = self
            .conn
            .prepare("SELECT id, count FROM task ORDER BY id")?;
        let mut results = Vec::new();
        let mut rows = q.query([])?;
        while let Some(row) = rows.next()? {
            let id = row.get(0).unwrap();
            let count: u64 = row.get(1).unwrap();
            let w = self.worker_count(id)?;
            if count > w {
                results.push(id);
            }
        }
        Ok(results)
    }

    pub fn get_data(&self, job_id: TaskId) -> Result<Vec<u8>> {
        let mut q = self.conn.prepare("SELECT data FROM task WHERE id = ?")?;
        let mut result = q.query([job_id])?;
        result.next()?.unwrap().get(0).map_err(From::from)
    }

    fn worker_count(&self, job_id: TaskId) -> Result<u64> {
        let mut q_w = self
            .conn
            .prepare("SELECT count(1) FROM job WHERE task = ?")?;
        let mut w = q_w.query([job_id])?;
        Ok(w.next()?.unwrap().get(0)?)
    }

    pub fn get_count(&self, job_id: TaskId) -> Result<u64> {
        let mut q_c = self.conn.prepare("SELECT count FROM task WHERE id = ?")?;
        let mut c = q_c.query([job_id])?;
        let c: u64 = c.next()?.unwrap().get(0)?;
        let w = self.worker_count(job_id)?;
        debug_assert!(c >= w);
        Ok(if w > c { 0 } else { c - w })
    }

    pub fn get_priority(&self, job_id: TaskId) -> Result<i32> {
        let mut q = self
            .conn
            .prepare("SELECT priority FROM task WHERE id = ?")?;
        let mut prio = q.query([job_id])?;
        let prio = prio.next()?.unwrap();
        let prio: Option<_> = prio.get(0)?;
        Ok(prio.unwrap_or(0))
    }

    pub fn current_job(&mut self, worker: &str) -> Result<Option<JobId>> {
        let mut q = self
            .conn
            .prepare("SELECT id FROM job WHERE worker = ? ORDER BY id DESC LIMIT 1")?;
        let mut rows = q.query([worker])?;
        Ok(match rows.next()? {
            Some(row) => row.get(0)?,
            None => None,
        })
    }

    pub fn log_start(&mut self, job: JobId, cmd: Vec<Vec<u8>>) -> Result<()> {
        let cmd = Command(cmd);
        self.conn.execute(
            "INSERT INTO job_start (job, time, cmd) VALUES (?, date('now'), ?)",
            params![job, cmd],
        )?;
        Ok(())
    }

    pub fn log_finish(&mut self, job: JobId, result: i32) -> Result<()> {
        self.conn.execute(
            "INSERT INTO job_finish (job, result, time) VALUES (?, ?, date('now'))",
            params![job, result],
        )?;
        Ok(())
    }

    pub fn get_jobs(&mut self) -> Result<Vec<JobId>> {
        let mut q = self.conn.prepare("SELECT id FROM job ORDER BY id")?;
        let mut results = Vec::new();
        let mut rows = q.query([])?;
        while let Some(row) = rows.next()? {
            results.push(row.get(0).unwrap());
        }
        Ok(results)
    }

    pub fn get_started_jobs(&mut self) -> Result<Vec<JobId>> {
        // get each worker's latest job
        let mut q0 = self
            .conn
            .prepare("SELECT MAX(id) FROM job GROUP BY worker ORDER BY id")?;
        let mut worker_latest = q0.query([])?;
        let mut results = Vec::new();
        while let Some(wl) = worker_latest.next()? {
            let job = wl.get(0).unwrap();
            // check if the job is started and not finished
            let q = "SELECT 1 \
                     FROM job_start \
                     LEFT JOIN job_finish \
                     ON job_start.job = job_finish.job \
                     WHERE job_finish.job IS NULL \
                     AND job_start.job = ?";
            let mut q = self.conn.prepare(q)?;
            let is_started = q.query([job])?.next()?.is_some();
            if is_started {
                results.push(job);
            }
        }
        Ok(results)
    }

    pub fn get_job_worker(&self, job: JobId) -> Result<String> {
        Ok(self
            .conn
            .prepare("SELECT worker FROM job WHERE id = ?")?
            .query([job])?
            .next()?
            .expect("JobId does not exist")
            .get(0)
            .unwrap())
    }

    pub fn get_worker_latest_job(&self, worker: &str) -> Result<Option<JobId>> {
        Ok(self
            .conn
            .prepare("SELECT id FROM job WHERE worker = ? ORDER BY id DESC")?
            .query([&worker])?
            .next()?
            .unwrap()
            .get(0)?)
    }

    pub fn get_job_start(&self, job: JobId) -> Result<Option<Start>> {
        Ok(self
            .conn
            .prepare("SELECT time, cmd FROM job_start WHERE job = ?")?
            .query([job])?
            .next()?
            .map(|row| Start {
                time: Time(row.get(0).unwrap()),
                cmd: row.get(1).unwrap(),
            }))
    }

    pub fn get_job_finish(&self, job: JobId) -> Result<Option<Finish>> {
        Ok(self
            .conn
            .prepare("SELECT time, result, data FROM job_finish WHERE job = ?")?
            .query([job])?
            .next()?
            .map(|row| Finish {
                time: Time(row.get(0).unwrap()),
                result: row.get(1).unwrap(),
                data: row.get(2).unwrap(),
            }))
    }
}

mod time_ {
    use std::fmt;
    use time::{OffsetDateTime, UtcOffset};

    #[derive(Copy, Clone)]
    pub struct Time(pub i64);

    impl fmt::Display for Time {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            let mut t = match OffsetDateTime::from_unix_timestamp(self.0) {
                Ok(t) => t,
                Err(_) => return write!(f, "<invalid timestamp>"),
            };
            if let Ok(tz) = UtcOffset::current_local_offset() {
                t = t.to_offset(tz);
            }
            write!(f, "{}", t)
        }
    }
}
pub use time_::Time;

#[derive(Serialize, Deserialize)]
pub struct Command(Vec<Vec<u8>>);

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut args = self.0.iter();
        let arg = match args.next() {
            Some(arg) => arg,
            None => return Ok(()),
        };
        match std::str::from_utf8(arg) {
            Ok(s) => write!(f, "{:?}", s)?,
            Err(_) => write!(f, "<binary>")?,
        }
        for arg in args {
            match std::str::from_utf8(&arg) {
                Ok(s) => write!(f, " {:?}", s)?,
                Err(_) => write!(f, " <binary>")?,
            }
        }
        Ok(())
    }
}

impl FromSql for Command {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        Vec::<u8>::column_result(value).and_then(|blob| {
            bincode::deserialize(&blob)
                .map(|vec| Command(vec))
                .map_err(|e| FromSqlError::Other(Box::new(e)))
        })
    }
}

impl ToSql for Command {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
        Ok(bincode::serialize(&self.0)
            .map_err(|e| FromSqlError::Other(Box::new(e)))?
            .into())
    }
}

pub struct Start {
    pub time: Time,
    pub cmd: Command,
}

pub struct Finish {
    pub result: i32,
    pub time: Time,
    pub data: Vec<u8>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_init_twice() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let db1 = Db::create_from_conn(conn)?;
        let result = Db::create_from_conn(db1.conn);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_db_too_new() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let db = Db::create_from_conn(conn)?;
        let conn = db.conn;
        conn.execute("UPDATE meta SET version = ?", [std::i32::MAX])?;
        let result = Db::open_from_conn(conn);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_job() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let mut db = Db::create_from_conn(conn)?;

        // insert a job
        const BLOB: &[u8] = b"foo bar";
        const INITIAL_COUNT: u64 = 2;
        let id = db.new_job(BLOB, INITIAL_COUNT, None)?;

        // make sure it's inserted
        let ids = db.job_ids_vec()?;
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], id);

        // make sure it's inserted correctly
        let blob = db.get_data(id)?;
        assert_eq!(&blob, BLOB);
        let count = db.get_count(id)?;
        assert_eq!(count, INITIAL_COUNT);

        // check that take() works
        let job = db.take("some worker id")?.unwrap();
        assert_eq!(job.id, id);
        assert_eq!(db.get_count(id)?, 1);
        let job = db.take("some worker id")?.unwrap();
        assert_eq!(job.id, id);
        assert_eq!(db.get_count(id)?, 0);
        let job = db.take("some worker id")?;
        assert_eq!(job, None);
        assert_eq!(db.get_count(id)?, 0);
        assert_eq!(db.job_ids_vec()?.len(), 0);

        Ok(())
    }

    #[test]
    fn test_logging() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let mut db = Db::create_from_conn(conn)?;

        // insert a job
        const BLOB: &[u8] = b"foo bar";
        const INITIAL_COUNT: u64 = 12;
        db.new_job(BLOB, INITIAL_COUNT, None)?;

        assert_eq!(db.get_started_jobs()?.len(), 0);
        let job = db.take("worker id")?.unwrap();
        assert_eq!(db.get_started_jobs()?.len(), 0);
        db.log_start(job.id, vec![])?;
        assert_eq!(db.get_started_jobs()?.len(), 1);
        db.log_finish(job.id, 0)?;
        assert_eq!(db.get_started_jobs()?.len(), 0);

        Ok(())
    }

    #[test]
    fn test_priority() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let mut db = Db::create_from_conn(conn)?;

        // some job creation
        let id_defaultgroup = db.new_job(b"default group", 1, None)?;
        let id_firstgroup0 = db.new_job(b"firstgroup 0", 2, Some(-10))?;
        let id_firstgroup1 = db.new_job(b"firstgroup 1", 1, Some(-10))?;

        // should round-robin through the lowest-priority group
        assert_eq!(db.take("worker id")?.unwrap().id, id_firstgroup0);
        assert_eq!(db.take("worker id")?.unwrap().id, id_firstgroup0);
        assert_eq!(db.take("worker id")?.unwrap().id, id_firstgroup1);

        // then do the mid-priority group
        assert_eq!(db.take("worker id")?.unwrap().id, id_defaultgroup);

        Ok(())
    }

    #[test]
    #[ignore]
    fn test_order() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let mut db = Db::create_from_conn(conn)?;

        let id0 = db.new_job(b"firstgroup 0", 2, Some(-10))?;
        let id1 = db.new_job(b"firstgroup 1", 1, Some(-10))?;

        // should round-robin through the lowest-priority group
        assert_eq!(db.take("worker id")?.unwrap().id, id0);
        assert_eq!(db.take("worker id")?.unwrap().id, id0);
        assert_eq!(db.take("worker id")?.unwrap().id, id1);
        Ok(())
    }
}
