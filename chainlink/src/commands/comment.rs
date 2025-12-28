use anyhow::{bail, Result};

use crate::db::Database;

pub fn run(db: &Database, issue_id: i64, content: &str) -> Result<()> {
    // Check if issue exists
    if db.get_issue(issue_id)?.is_none() {
        bail!("Issue #{} not found", issue_id);
    }

    db.add_comment(issue_id, content)?;
    println!("Added comment to issue #{}", issue_id);
    Ok(())
}
