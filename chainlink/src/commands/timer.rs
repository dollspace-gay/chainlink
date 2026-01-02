use anyhow::{bail, Result};
use chrono::Utc;

use crate::db::Database;

pub fn start(db: &Database, issue_id: i64) -> Result<()> {
    // Verify issue exists
    let issue = db.get_issue(issue_id)?;
    if issue.is_none() {
        bail!("Issue #{} not found", issue_id);
    }
    let issue = issue.unwrap();

    // Check if there's already an active timer
    if let Some((active_id, _)) = db.get_active_timer()? {
        if active_id == issue_id {
            bail!("Timer already running for issue #{}", issue_id);
        } else {
            bail!(
                "Timer already running for issue #{}. Stop it first with 'chainlink stop'.",
                active_id
            );
        }
    }

    db.start_timer(issue_id)?;
    println!("Started timer for #{}: {}", issue_id, issue.title);
    println!("Run 'chainlink stop' when done.");

    Ok(())
}

pub fn stop(db: &Database) -> Result<()> {
    let active = db.get_active_timer()?;
    if active.is_none() {
        bail!("No timer running. Start one with 'chainlink start <id>'.");
    }

    let (issue_id, started_at) = active.unwrap();
    let duration = Utc::now().signed_duration_since(started_at);

    db.stop_timer(issue_id)?;

    let issue = db.get_issue(issue_id)?;
    let title = issue
        .map(|i| i.title)
        .unwrap_or_else(|| "(deleted)".to_string());

    let hours = duration.num_hours();
    let minutes = duration.num_minutes() % 60;
    let seconds = duration.num_seconds() % 60;

    println!("Stopped timer for #{}: {}", issue_id, title);
    println!("Time spent: {}h {}m {}s", hours, minutes, seconds);

    // Show total time for this issue
    let total = db.get_total_time(issue_id)?;
    let total_hours = total / 3600;
    let total_minutes = (total % 3600) / 60;
    println!(
        "Total time on this issue: {}h {}m",
        total_hours, total_minutes
    );

    Ok(())
}

pub fn status(db: &Database) -> Result<()> {
    let active = db.get_active_timer()?;

    match active {
        Some((issue_id, started_at)) => {
            let duration = Utc::now().signed_duration_since(started_at);
            let hours = duration.num_hours();
            let minutes = duration.num_minutes() % 60;
            let seconds = duration.num_seconds() % 60;

            let issue = db.get_issue(issue_id)?;
            let title = issue
                .map(|i| i.title)
                .unwrap_or_else(|| "(deleted)".to_string());

            println!("Timer running: #{} {}", issue_id, title);
            println!("Elapsed: {}h {}m {}s", hours, minutes, seconds);
        }
        None => {
            println!("No timer running.");
        }
    }

    Ok(())
}
