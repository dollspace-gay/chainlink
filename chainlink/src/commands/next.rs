use anyhow::Result;

use crate::db::Database;
use crate::models::Issue;

/// Progress tuple: (completed subissues, total subissues)
type Progress = Option<(i32, i32)>;

/// Scored issue with priority score and progress
type ScoredIssue = (Issue, i32, Progress);

/// Priority order for sorting (higher = more important)
fn priority_weight(priority: &str) -> i32 {
    match priority {
        "critical" => 4,
        "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 0,
    }
}

/// Calculate progress for issues with subissues
fn calculate_progress(db: &Database, issue: &Issue) -> Result<Progress> {
    let subissues = db.get_subissues(issue.id)?;
    if subissues.is_empty() {
        return Ok(None);
    }

    let total = subissues.len() as i32;
    let closed = subissues.iter().filter(|s| s.status == "closed").count() as i32;
    Ok(Some((closed, total)))
}

pub fn run(db: &Database) -> Result<()> {
    let ready = db.list_ready_issues()?;

    if ready.is_empty() {
        println!("No issues ready to work on.");
        println!(
            "Use 'chainlink list' to see all issues or 'chainlink blocked' to see blocked issues."
        );
        return Ok(());
    }

    // Score and sort issues
    let mut scored: Vec<ScoredIssue> = Vec::new();

    for issue in ready {
        // Skip subissues - we want to recommend parent issues or standalone issues
        if issue.parent_id.is_some() {
            continue;
        }

        let priority_score = priority_weight(&issue.priority) * 100;
        let progress = calculate_progress(db, &issue)?;

        // Boost score for issues that are partially complete (finish what you started)
        let progress_bonus = match &progress {
            Some((closed, total)) if *closed > 0 && *closed < *total => 50,
            _ => 0,
        };

        let score = priority_score + progress_bonus;
        scored.push((issue, score, progress));
    }

    // Sort by score descending
    scored.sort_by(|a, b| b.1.cmp(&a.1));

    if scored.is_empty() {
        // All ready issues are subissues, show them instead
        let ready = db.list_ready_issues()?;
        if let Some(issue) = ready.first() {
            println!("Next: #{} [{}] {}", issue.id, issue.priority, issue.title);
            if let Some(parent_id) = issue.parent_id {
                println!("       (subissue of #{})", parent_id);
            }
        } else {
            println!("No issues ready to work on.");
        }
        return Ok(());
    }

    // Recommend the top issue
    let (top, _score, progress) = &scored[0];
    println!("Next: #{} [{}] {}", top.id, top.priority, top.title);

    if let Some((closed, total)) = progress {
        println!("       Progress: {}/{} subissues complete", closed, total);
    }

    if let Some(desc) = &top.description {
        if !desc.is_empty() {
            let preview: String = desc.chars().take(80).collect();
            let suffix = if desc.len() > 80 { "..." } else { "" };
            println!("       {}{}", preview, suffix);
        }
    }

    println!();
    println!("Run: chainlink session work {}", top.id);

    // Show runners-up if any
    if scored.len() > 1 {
        println!();
        println!("Also ready:");
        for (issue, _score, progress) in scored.iter().skip(1).take(3) {
            let progress_str = match progress {
                Some((c, t)) => format!(" ({}/{})", c, t),
                None => String::new(),
            };
            println!(
                "  #{} [{}] {}{}",
                issue.id, issue.priority, issue.title, progress_str
            );
        }
    }

    Ok(())
}
