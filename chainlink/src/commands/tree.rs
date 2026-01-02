use anyhow::Result;

use crate::db::Database;
use crate::models::Issue;

fn status_icon(status: &str) -> &'static str {
    match status {
        "open" => " ",
        "closed" => "x",
        _ => "?",
    }
}

fn print_issue(issue: &Issue, indent: usize) {
    let prefix = "  ".repeat(indent);
    let icon = status_icon(&issue.status);
    println!(
        "{}[{}] #{} {} - {}",
        prefix, icon, issue.id, issue.priority, issue.title
    );
}

fn print_tree_recursive(db: &Database, parent_id: i64, indent: usize) -> Result<()> {
    let subissues = db.get_subissues(parent_id)?;
    for sub in subissues {
        print_issue(&sub, indent);
        print_tree_recursive(db, sub.id, indent + 1)?;
    }
    Ok(())
}

pub fn run(db: &Database, status_filter: Option<&str>) -> Result<()> {
    // Get all top-level issues (no parent)
    let all_issues = db.list_issues(status_filter, None, None)?;
    let top_level: Vec<_> = all_issues
        .into_iter()
        .filter(|i| i.parent_id.is_none())
        .collect();

    if top_level.is_empty() {
        println!("No issues found.");
        return Ok(());
    }

    for issue in top_level {
        print_issue(&issue, 0);
        print_tree_recursive(db, issue.id, 1)?;
    }

    // Legend
    println!();
    println!("Legend: [ ] open, [x] closed");

    Ok(())
}
