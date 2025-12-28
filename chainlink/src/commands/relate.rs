use anyhow::{bail, Result};

use crate::db::Database;

pub fn add(db: &Database, issue_id: i64, related_id: i64) -> Result<()> {
    // Verify both issues exist
    if db.get_issue(issue_id)?.is_none() {
        bail!("Issue #{} not found", issue_id);
    }
    if db.get_issue(related_id)?.is_none() {
        bail!("Issue #{} not found", related_id);
    }

    if db.add_relation(issue_id, related_id)? {
        println!("Linked #{} ↔ #{}", issue_id, related_id);
    } else {
        println!("Issues #{} and #{} are already related", issue_id, related_id);
    }

    Ok(())
}

pub fn remove(db: &Database, issue_id: i64, related_id: i64) -> Result<()> {
    if db.remove_relation(issue_id, related_id)? {
        println!("Unlinked #{} ↔ #{}", issue_id, related_id);
    } else {
        println!("No relation found between #{} and #{}", issue_id, related_id);
    }

    Ok(())
}

pub fn list(db: &Database, issue_id: i64) -> Result<()> {
    let issue = db.get_issue(issue_id)?;
    if issue.is_none() {
        bail!("Issue #{} not found", issue_id);
    }

    let related = db.get_related_issues(issue_id)?;

    if related.is_empty() {
        println!("No related issues for #{}", issue_id);
        return Ok(());
    }

    println!("Related to #{}:", issue_id);
    for r in related {
        let status_marker = if r.status == "closed" { "✓" } else { " " };
        println!("  #{:<4} [{}] {:8} {}", r.id, status_marker, r.priority, r.title);
    }

    Ok(())
}
