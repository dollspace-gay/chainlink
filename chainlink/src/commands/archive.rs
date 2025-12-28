use anyhow::{bail, Result};

use crate::db::Database;

pub fn archive(db: &Database, id: i64) -> Result<()> {
    let issue = db.get_issue(id)?;
    if issue.is_none() {
        bail!("Issue #{} not found", id);
    }

    let issue = issue.unwrap();
    if issue.status != "closed" {
        bail!("Can only archive closed issues. Issue #{} is '{}'", id, issue.status);
    }

    if db.archive_issue(id)? {
        println!("Archived issue #{}", id);
    } else {
        println!("Issue #{} could not be archived", id);
    }

    Ok(())
}

pub fn unarchive(db: &Database, id: i64) -> Result<()> {
    if db.unarchive_issue(id)? {
        println!("Unarchived issue #{} (now closed)", id);
    } else {
        bail!("Issue #{} not found or not archived", id);
    }

    Ok(())
}

pub fn list(db: &Database) -> Result<()> {
    let issues = db.list_archived_issues()?;

    if issues.is_empty() {
        println!("No archived issues.");
        return Ok(());
    }

    println!("Archived issues:\n");
    for issue in issues {
        let parent_str = issue.parent_id.map(|p| format!(" (sub of #{})", p)).unwrap_or_default();
        println!("#{:<4} {:8} {}{}", issue.id, issue.priority, issue.title, parent_str);
    }

    Ok(())
}

pub fn archive_older(db: &Database, days: i64) -> Result<()> {
    let count = db.archive_older_than(days)?;
    if count > 0 {
        println!("Archived {} issue(s) closed more than {} days ago", count, days);
    } else {
        println!("No issues to archive (none closed more than {} days ago)", days);
    }

    Ok(())
}
