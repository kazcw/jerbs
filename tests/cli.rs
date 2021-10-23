use anyhow::Result;
use assert_cmd::Command;
use std::path::Path;
use tempfile::NamedTempFile;

fn cmd(db: &Path, args: &[&str]) -> Result<Command> {
    let mut cmd = Command::cargo_bin("jerbs")?;
    cmd.arg(db);
    cmd.args(args);
    Ok(cmd)
}

#[test]
fn test_init() -> Result<()> {
    let db_file = NamedTempFile::new()?;
    let db = db_file.path();
    cmd(db, &["init"])?.assert().success();
    Ok(())
}

#[test]
fn test_take() -> Result<()> {
    let db_file = NamedTempFile::new()?;
    let db = db_file.path();
    cmd(db, &["init"])?.assert().success();
    // create a one-shot job
    cmd(db, &["create", "-c", "1", "-d", "JOBDATA"])?
        .assert()
        .success();
    // take it
    cmd(db, &["take", "WORKERDATA"])?.assert().success();
    // can't take it again
    cmd(db, &["take", "WORKERDATA"])?.assert().failure();
    Ok(())
}

#[test]
fn test_monitor() -> Result<()> {
    let db_file = NamedTempFile::new()?;
    let db = db_file.path();
    cmd(db, &["init"])?.assert().success();
    // create a job with many instances
    cmd(db, &["create", "-c", "12", "-d", "JOBDATA"])?
        .assert()
        .success();
    // worker with no job shouldn't be able to log
    cmd(db, &["log-start", "WORKERDATA1"])?.assert().failure();
    cmd(db, &["log-finish", "WORKERDATA1", "0"])?
        .assert()
        .failure();
    // get a job and then try
    cmd(db, &["take", "WORKERDATA1"])?.assert().success();
    cmd(db, &["log-start", "WORKERDATA1"])?.assert().success();
    cmd(db, &["log-finish", "WORKERDATA1", "0"])?
        .assert()
        .success();
    // no active jobs once again, shouldn't be ready to log
    cmd(db, &["log-start", "WORKERDATA1"])?.assert().failure();
    cmd(db, &["log-finish", "WORKERDATA1", "0"])?
        .assert()
        .failure();
    // monitor should be effectively the same as log-start / log-finish
    cmd(db, &["take", "WORKERDATA1"])?.assert().success();
    cmd(db, &["monitor", "WORKERDATA1", "--", "true"])?
        .assert()
        .success();
    cmd(db, &["log-finish", "WORKERDATA1", "0"])?
        .assert()
        .failure();
    // non-existent command should fail, and log start and finish
    cmd(db, &["take", "WORKERDATA1"])?.assert().success();
    cmd(
        db,
        &["monitor", "WORKERDATA1", "--", "nosuchcommand_foobarbaz"],
    )?
    .assert()
    .failure();
    cmd(db, &["log-start", "WORKERDATA1"])?.assert().failure();
    cmd(db, &["log-finish", "WORKERDATA1", "0"])?
        .assert()
        .failure();
    // monitoring command that returns failure should return failure, and log start and finish
    cmd(db, &["take", "WORKERDATA1"])?.assert().success();
    cmd(db, &["monitor", "WORKERDATA1", "--", "false"])?
        .assert()
        .failure();
    cmd(db, &["log-start", "WORKERDATA1"])?.assert().failure();
    cmd(db, &["log-finish", "WORKERDATA1", "0"])?
        .assert()
        .failure();
    Ok(())
}
