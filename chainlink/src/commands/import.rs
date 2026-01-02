use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use super::export::{ExportData, ExportedIssue};
use crate::db::Database;

pub fn run_json(db: &Database, input_path: &Path) -> Result<()> {
    let content = fs::read_to_string(input_path).context("Failed to read import file")?;

    let data: ExportData = serde_json::from_str(&content).context("Failed to parse JSON")?;

    println!(
        "Importing {} issues from {}",
        data.issues.len(),
        input_path.display()
    );

    // Map old IDs to new IDs for parent relationships
    let mut id_map: HashMap<i64, i64> = HashMap::new();

    // First pass: create all issues without parent relationships
    for issue in &data.issues {
        let new_id = import_issue(db, issue, None)?;
        id_map.insert(issue.id, new_id);
    }

    // Second pass: update parent relationships
    for issue in &data.issues {
        if let Some(old_parent_id) = issue.parent_id {
            if let Some(&new_parent_id) = id_map.get(&old_parent_id) {
                if let Some(&new_id) = id_map.get(&issue.id) {
                    // Update parent_id for this issue
                    db.update_parent(new_id, Some(new_parent_id))?;
                }
            }
        }
    }

    println!("Successfully imported {} issues", data.issues.len());
    Ok(())
}

fn import_issue(db: &Database, issue: &ExportedIssue, parent_id: Option<i64>) -> Result<i64> {
    let id = if let Some(pid) = parent_id {
        db.create_subissue(
            pid,
            &issue.title,
            issue.description.as_deref(),
            &issue.priority,
        )?
    } else {
        db.create_issue(&issue.title, issue.description.as_deref(), &issue.priority)?
    };

    // Add labels
    for label in &issue.labels {
        db.add_label(id, label)?;
    }

    // Add comments
    for comment in &issue.comments {
        db.add_comment(id, &comment.content)?;
    }

    // Close if needed
    if issue.status == "closed" {
        db.close_issue(id)?;
    }

    println!("  Imported: #{} -> #{} {}", issue.id, id, issue.title);
    Ok(id)
}
