use anyhow::{bail, Result};

use crate::db::Database;

pub fn run(db: &Database, id: i64) -> Result<()> {
    let issue = match db.get_issue(id)? {
        Some(i) => i,
        None => bail!("Issue #{} not found", id),
    };

    println!("Issue #{}: {}", issue.id, issue.title);
    println!("Status: {}", issue.status);
    println!("Priority: {}", issue.priority);
    if let Some(parent_id) = issue.parent_id {
        println!("Parent: #{}", parent_id);
    }
    println!("Created: {}", issue.created_at.format("%Y-%m-%d %H:%M:%S"));
    println!("Updated: {}", issue.updated_at.format("%Y-%m-%d %H:%M:%S"));

    if let Some(closed) = issue.closed_at {
        println!("Closed: {}", closed.format("%Y-%m-%d %H:%M:%S"));
    }

    // Labels
    let labels = db.get_labels(id)?;
    if !labels.is_empty() {
        println!("Labels: {}", labels.join(", "));
    }

    // Milestone
    if let Some(milestone) = db.get_issue_milestone(id)? {
        println!("Milestone: #{} {}", milestone.id, milestone.name);
    }

    // Description
    if let Some(desc) = &issue.description {
        if !desc.is_empty() {
            println!("\nDescription:");
            for line in desc.lines() {
                println!("  {}", line);
            }
        }
    }

    // Comments
    let comments = db.get_comments(id)?;
    if !comments.is_empty() {
        println!("\nComments:");
        for comment in comments {
            println!(
                "  [{}] {}",
                comment.created_at.format("%Y-%m-%d %H:%M"),
                comment.content
            );
        }
    }

    // Dependencies
    let blockers = db.get_blockers(id)?;
    let blocking = db.get_blocking(id)?;

    println!();
    if blockers.is_empty() {
        println!("Blocked by: (none)");
    } else {
        let blocker_strs: Vec<String> = blockers.iter().map(|b| format!("#{}", b)).collect();
        println!("Blocked by: {}", blocker_strs.join(", "));
    }

    if blocking.is_empty() {
        println!("Blocking: (none)");
    } else {
        let blocking_strs: Vec<String> = blocking.iter().map(|b| format!("#{}", b)).collect();
        println!("Blocking: {}", blocking_strs.join(", "));
    }

    // Subissues
    let subissues = db.get_subissues(id)?;
    if !subissues.is_empty() {
        println!("\nSubissues:");
        for sub in subissues {
            println!(
                "  #{} [{}] {} - {}",
                sub.id, sub.status, sub.priority, sub.title
            );
        }
    }

    // Related issues
    let related = db.get_related_issues(id)?;
    if !related.is_empty() {
        println!("\nRelated:");
        for rel in related {
            let status_marker = if rel.status == "closed" { "✓" } else { " " };
            println!(
                "  #{} [{}] {} - {}",
                rel.id, status_marker, rel.priority, rel.title
            );
        }
    }

    Ok(())
}
