use anyhow::{bail, Result};

use crate::db::Database;

pub fn add(db: &Database, issue_id: i64, label: &str) -> Result<()> {
    // Check if issue exists
    if db.get_issue(issue_id)?.is_none() {
        bail!("Issue #{} not found", issue_id);
    }

    if db.add_label(issue_id, label)? {
        println!("Added label '{}' to issue #{}", label, issue_id);
    } else {
        println!("Label '{}' already exists on issue #{}", label, issue_id);
    }
    Ok(())
}

pub fn remove(db: &Database, issue_id: i64, label: &str) -> Result<()> {
    // Check if issue exists
    if db.get_issue(issue_id)?.is_none() {
        bail!("Issue #{} not found", issue_id);
    }

    if db.remove_label(issue_id, label)? {
        println!("Removed label '{}' from issue #{}", label, issue_id);
    } else {
        println!("Label '{}' not found on issue #{}", label, issue_id);
    }
    Ok(())
}
