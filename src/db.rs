use rusqlite::params;
use rusqlite::Connection;
use std::fmt::{self, Display};

const DB_VERSION: i32 = 1;

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    DbTooNew { db_version: i32 },
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

struct Job {
    id: i32,
    data: Vec<u8>,
}

pub struct Db {
    conn: Connection,
}

impl Db {
    pub fn create(path: &str) -> Result<Self> {
        // TODO: fail right away if the path exists--would give a clearer error message than
        // bailing on a CREATE TABLE below.
        let conn = Connection::open(path)?;

        Self::create_from_conn(conn)
    }

    fn create_from_conn(conn: Connection) -> Result<Self> {
        conn.execute("CREATE TABLE meta (version INTEGER)", [])?;
        conn.execute("CREATE TABLE job (id INTEGER PRIMARY KEY, count INTEGER NOT NULL, data BLOB NOT NULL UNIQUE)", [])?;
        conn.execute(
            "CREATE TABLE worker (id INTEGER PRIMARY KEY, job REFERENCES job, data BLOB NOT NULL)",
            [],
        )?;
        conn.execute("INSERT INTO meta VALUES (?)", [DB_VERSION])?;

        Ok(Self { conn })
    }

    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        Self::open_from_conn(conn)
    }

    fn open_from_conn(conn: Connection) -> Result<Self> {
        {
            let mut version = conn.prepare("SELECT version FROM meta")?;
            let mut version = version.query([])?;
            let version: i32 = version.next()?.unwrap().get(0)?;
            if version > DB_VERSION {
                return Err(Error::DbTooNew {
                    db_version: version,
                }
                .into());
            }
        }
        Ok(Self { conn })
    }

    pub fn take(&mut self, data: &str) -> Result<Option<Vec<u8>>> {
        const JOB_Q: &str = "SELECT job.id, job.data FROM job \
           LEFT JOIN (SELECT worker.job, count(1) as c FROM worker GROUP BY worker.job) as w
           ON w.job = job.id \
         WHERE COALESCE(w.c, 0) < job.count ORDER BY job.id LIMIT 1";
        let job;
        let tx = self.conn.transaction()?;
        {
            let mut job_q = tx.prepare(JOB_Q)?;
            let mut jobs = job_q.query([])?;
            let row = jobs.next()?;
            if let Some(row) = row {
                job = Job {
                    id: row.get(0)?,
                    data: row.get(1)?,
                };
            } else {
                return Ok(None);
            }
            tx.execute(
                "INSERT INTO worker (job, data) VALUES (?, ?)",
                params![job.id, data],
            )?;
        }
        tx.commit()?;

        Ok(Some(job.data))
    }

    pub fn new_job(&mut self, data: &[u8], count: u64) -> Result<u32> {
        self.conn.execute(
            "INSERT INTO job (data, count) VALUES (?, ?)",
            params![data, count],
        )?;
        let id = self.conn.last_insert_rowid() as u32;

        Ok(id)
    }

    // TODO: iterator version. Has to own its Statement.
    pub fn job_ids_vec(&self) -> Result<Vec<u32>> {
        let mut q = self.conn.prepare("SELECT id, count FROM job")?;
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

    pub fn get_data(&self, job_id: u32) -> Result<Vec<u8>> {
        let mut q = self.conn.prepare("SELECT data FROM job WHERE id = ?")?;
        let mut result = q.query([job_id])?;
        result.next()?.unwrap().get(0).map_err(From::from)
    }

    fn worker_count(&self, job_id: u32) -> Result<u64> {
        let mut q_w = self
            .conn
            .prepare("SELECT count(1) FROM worker WHERE job = ?")?;
        let mut w = q_w.query([job_id])?;
        Ok(w.next()?.unwrap().get(0)?)
    }

    pub fn get_count(&self, job_id: u32) -> Result<u64> {
        let mut q_c = self.conn.prepare("SELECT count FROM job WHERE id = ?")?;
        let mut c = q_c.query([job_id])?;
        let c: u64 = c.next()?.unwrap().get(0)?;
        let w = self.worker_count(job_id)?;
        debug_assert!(c >= w);
        Ok(if w > c { 0 } else { c - w })
    }
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
        let id = db.new_job(BLOB, INITIAL_COUNT)?;

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
        let blob = db.take("some worker id")?.unwrap();
        assert_eq!(&blob, BLOB);
        assert_eq!(db.get_count(id)?, 1);
        let blob = db.take("some worker id")?.unwrap();
        assert_eq!(&blob, BLOB);
        assert_eq!(db.get_count(id)?, 0);
        let result = db.take("some worker id")?;
        assert_eq!(result, None);
        assert_eq!(db.get_count(id)?, 0);
        assert_eq!(db.job_ids_vec()?.len(), 0);

        Ok(())
    }

    #[test]
    fn test_job_collision() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let mut db = Db::create_from_conn(conn)?;

        // insert a job
        const BLOB: &[u8] = b"foo bar";
        db.new_job(BLOB, 3)?;

        // try to insert another job with the same blob
        let secondtime = db.new_job(BLOB, 0);
        assert!(secondtime.is_err());

        Ok(())
    }
}
