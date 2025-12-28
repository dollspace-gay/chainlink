use anyhow::{bail, Result};

use crate::db::Database;

pub fn create(db: &Database, name: &str, description: Option<&str>) -> Result<()> {
    let id = db.create_milestone(name, description)?;
    println!("Created milestone #{}: {}", id, name);
    Ok(())
}

pub fn list(db: &Database, status: Option<&str>) -> Result<()> {
    let milestones = db.list_milestones(status)?;

    if milestones.is_empty() {
        println!("No milestones found.");
        return Ok(());
    }

    for m in milestones {
        let issues = db.get_milestone_issues(m.id)?;
        let total = issues.len();
        let closed = issues.iter().filter(|i| i.status == "closed").count();
        let progress = if total > 0 {
            format!("{}/{}", closed, total)
        } else {
            "0/0".to_string()
        };

        let status_marker = if m.status == "closed" { "✓" } else { " " };
        println!("#{:<3} [{}] {} ({})", m.id, status_marker, m.name, progress);
    }

    Ok(())
}

pub fn show(db: &Database, id: i64) -> Result<()> {
    let milestone = db.get_milestone(id)?;
    if milestone.is_none() {
        bail!("Milestone #{} not found", id);
    }

    let m = milestone.unwrap();
    println!("Milestone #{}: {}", m.id, m.name);
    println!("Status: {}", m.status);
    println!("Created: {}", m.created_at.format("%Y-%m-%d %H:%M:%S"));

    if let Some(closed) = m.closed_at {
        println!("Closed: {}", closed.format("%Y-%m-%d %H:%M:%S"));
    }

    if let Some(ref desc) = m.description {
        if !desc.is_empty() {
            println!("\nDescription:");
            for line in desc.lines() {
                println!("  {}", line);
            }
        }
    }

    let issues = db.get_milestone_issues(id)?;
    let total = issues.len();
    let closed = issues.iter().filter(|i| i.status == "closed").count();

    println!("\nProgress: {}/{} issues closed", closed, total);

    if !issues.is_empty() {
        println!("\nIssues:");
        for issue in issues {
            let status_marker = if issue.status == "closed" { "✓" } else { " " };
            println!(
                "  #{:<4} [{}] {:8} {}",
                issue.id, status_marker, issue.priority, issue.title
            );
        }
    }

    Ok(())
}

pub fn add(db: &Database, milestone_id: i64, issue_ids: &[i64]) -> Result<()> {
    let milestone = db.get_milestone(milestone_id)?;
    if milestone.is_none() {
        bail!("Milestone #{} not found", milestone_id);
    }

    for &issue_id in issue_ids {
        if db.get_issue(issue_id)?.is_none() {
            println!("Warning: Issue #{} not found, skipping", issue_id);
            continue;
        }

        if db.add_issue_to_milestone(milestone_id, issue_id)? {
            println!("Added #{} to milestone #{}", issue_id, milestone_id);
        } else {
            println!("Issue #{} already in milestone #{}", issue_id, milestone_id);
        }
    }

    Ok(())
}

pub fn remove(db: &Database, milestone_id: i64, issue_id: i64) -> Result<()> {
    if db.remove_issue_from_milestone(milestone_id, issue_id)? {
        println!("Removed #{} from milestone #{}", issue_id, milestone_id);
    } else {
        println!("Issue #{} not in milestone #{}", issue_id, milestone_id);
    }

    Ok(())
}

pub fn close(db: &Database, id: i64) -> Result<()> {
    if db.close_milestone(id)? {
        println!("Closed milestone #{}", id);
    } else {
        println!("Milestone #{} not found", id);
    }

    Ok(())
}

pub fn delete(db: &Database, id: i64) -> Result<()> {
    if db.delete_milestone(id)? {
        println!("Deleted milestone #{}", id);
    } else {
        println!("Milestone #{} not found", id);
    }

    Ok(())
}
