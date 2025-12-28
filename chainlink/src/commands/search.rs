use anyhow::Result;

use crate::db::Database;

pub fn run(db: &Database, query: &str) -> Result<()> {
    let results = db.search_issues(query)?;

    if results.is_empty() {
        println!("No issues found matching '{}'", query);
        return Ok(());
    }

    println!("Found {} issue(s) matching '{}':\n", results.len(), query);

    for issue in results {
        let status_marker = if issue.status == "closed" { "✓" } else { " " };
        let parent_str = issue.parent_id.map(|p| format!(" (sub of #{})", p)).unwrap_or_default();

        println!(
            "#{:<4} [{}] {:8} {}{} {}",
            issue.id,
            status_marker,
            issue.priority,
            issue.title,
            parent_str,
            if issue.status == "closed" { "(closed)" } else { "" }
        );

        // Show snippet of description if it contains the query
        if let Some(ref desc) = issue.description {
            if desc.to_lowercase().contains(&query.to_lowercase()) {
                let preview: String = desc.chars().take(60).collect();
                let suffix = if desc.len() > 60 { "..." } else { "" };
                println!("      └─ {}{}", preview.replace('\n', " "), suffix);
            }
        }
    }

    Ok(())
}
