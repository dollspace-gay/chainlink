use anyhow::Result;

use crate::db::Database;

pub fn run(
    db: &Database,
    status: Option<&str>,
    label: Option<&str>,
    priority: Option<&str>,
) -> Result<()> {
    let issues = db.list_issues(status, label, priority)?;

    if issues.is_empty() {
        println!("No issues found.");
        return Ok(());
    }

    for issue in issues {
        let status_display = format!("[{}]", issue.status);
        let date = issue.created_at.format("%Y-%m-%d");
        println!(
            "#{:<4} {:8} {:<40} {:8} {}",
            issue.id,
            status_display,
            truncate(&issue.title, 40),
            issue.priority,
            date
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
