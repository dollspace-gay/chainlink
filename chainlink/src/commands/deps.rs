use anyhow::{bail, Result};

use crate::db::Database;

pub fn block(db: &Database, issue_id: i64, blocker_id: i64) -> Result<()> {
    // Check if both issues exist
    if db.get_issue(issue_id)?.is_none() {
        bail!("Issue #{} not found", issue_id);
    }
    if db.get_issue(blocker_id)?.is_none() {
        bail!("Issue #{} not found", blocker_id);
    }

    if issue_id == blocker_id {
        bail!("An issue cannot block itself");
    }

    if db.add_dependency(issue_id, blocker_id)? {
        println!("Issue #{} is now blocked by #{}", issue_id, blocker_id);
    } else {
        println!("Dependency already exists");
    }
    Ok(())
}

pub fn unblock(db: &Database, issue_id: i64, blocker_id: i64) -> Result<()> {
    if db.remove_dependency(issue_id, blocker_id)? {
        println!("Removed: #{} no longer blocked by #{}", issue_id, blocker_id);
    } else {
        println!("No such dependency found");
    }
    Ok(())
}

pub fn list_blocked(db: &Database) -> Result<()> {
    let issues = db.list_blocked_issues()?;

    if issues.is_empty() {
        println!("No blocked issues.");
        return Ok(());
    }

    println!("Blocked issues:");
    for issue in issues {
        let blockers = db.get_blockers(issue.id)?;
        let blocker_strs: Vec<String> = blockers.iter().map(|b| format!("#{}", b)).collect();
        println!(
            "  #{:<4} {} (blocked by: {})",
            issue.id,
            truncate(&issue.title, 40),
            blocker_strs.join(", ")
        );
    }

    Ok(())
}

pub fn list_ready(db: &Database) -> Result<()> {
    let issues = db.list_ready_issues()?;

    if issues.is_empty() {
        println!("No ready issues.");
        return Ok(());
    }

    println!("Ready issues (no blockers):");
    for issue in issues {
        println!(
            "  #{:<4} {:8} {}",
            issue.id, issue.priority, issue.title
        );
    }

    Ok(())
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}
