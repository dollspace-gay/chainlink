use anyhow::{bail, Result};
use chrono::Utc;

use crate::db::Database;

pub fn start(db: &Database) -> Result<()> {
    // Check if there's already an active session
    if let Some(current) = db.get_current_session()? {
        println!(
            "Session #{} is already active (started {})",
            current.id,
            current.started_at.format("%Y-%m-%d %H:%M")
        );
        return Ok(());
    }

    // Show previous session's handoff notes
    if let Some(last) = db.get_last_session()? {
        if let Some(ended) = last.ended_at {
            println!("Previous session ended: {}", ended.format("%Y-%m-%d %H:%M"));
        }
        if let Some(notes) = &last.handoff_notes {
            if !notes.is_empty() {
                println!("Handoff notes:");
                for line in notes.lines() {
                    println!("  {}", line);
                }
                println!();
            }
        }
    }

    let id = db.start_session()?;
    println!("Session #{} started.", id);
    Ok(())
}

pub fn end(db: &Database, notes: Option<&str>) -> Result<()> {
    let session = match db.get_current_session()? {
        Some(s) => s,
        None => bail!("No active session"),
    };

    db.end_session(session.id, notes)?;
    println!("Session #{} ended.", session.id);
    if notes.is_some() {
        println!("Handoff notes saved.");
    }
    Ok(())
}

pub fn status(db: &Database) -> Result<()> {
    let session = match db.get_current_session()? {
        Some(s) => s,
        None => {
            println!("No active session. Use 'chainlink session start' to begin.");
            return Ok(());
        }
    };

    let duration = Utc::now() - session.started_at;
    let minutes = duration.num_minutes();

    println!(
        "Session #{} (started {})",
        session.id,
        session.started_at.format("%Y-%m-%d %H:%M")
    );

    if let Some(issue_id) = session.active_issue_id {
        if let Some(issue) = db.get_issue(issue_id)? {
            println!("Working on: #{} {}", issue.id, issue.title);
        } else {
            println!("Working on: #{} (issue not found)", issue_id);
        }
    } else {
        println!("Working on: (none)");
    }

    println!("Duration: {} minutes", minutes);
    Ok(())
}

pub fn work(db: &Database, issue_id: i64) -> Result<()> {
    let session = match db.get_current_session()? {
        Some(s) => s,
        None => bail!("No active session. Use 'chainlink session start' first."),
    };

    let issue = match db.get_issue(issue_id)? {
        Some(i) => i,
        None => bail!("Issue #{} not found", issue_id),
    };

    db.set_session_issue(session.id, issue_id)?;
    println!("Now working on: #{} {}", issue.id, issue.title);
    Ok(())
}
