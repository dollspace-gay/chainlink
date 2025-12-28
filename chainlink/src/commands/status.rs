use anyhow::{bail, Result};

use crate::db::Database;

pub fn close(db: &Database, id: i64) -> Result<()> {
    if db.close_issue(id)? {
        println!("Closed issue #{}", id);
    } else {
        bail!("Issue #{} not found", id);
    }
    Ok(())
}

pub fn reopen(db: &Database, id: i64) -> Result<()> {
    if db.reopen_issue(id)? {
        println!("Reopened issue #{}", id);
    } else {
        bail!("Issue #{} not found", id);
    }
    Ok(())
}
