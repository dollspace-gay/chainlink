use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

use crate::db::Database;
use crate::models::Issue;

#[derive(Serialize, Deserialize)]
pub struct ExportedIssue {
    pub id: i64,
    pub title: String,
    pub description: Option<String>,
    pub status: String,
    pub priority: String,
    pub parent_id: Option<i64>,
    pub labels: Vec<String>,
    pub comments: Vec<ExportedComment>,
    pub created_at: String,
    pub updated_at: String,
    pub closed_at: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ExportedComment {
    pub content: String,
    pub created_at: String,
}

#[derive(Serialize, Deserialize)]
pub struct ExportData {
    pub version: i32,
    pub exported_at: String,
    pub issues: Vec<ExportedIssue>,
}

fn export_issue(db: &Database, issue: &Issue) -> Result<ExportedIssue> {
    let labels = db.get_labels(issue.id)?;
    let comments = db.get_comments(issue.id)?;

    Ok(ExportedIssue {
        id: issue.id,
        title: issue.title.clone(),
        description: issue.description.clone(),
        status: issue.status.clone(),
        priority: issue.priority.clone(),
        parent_id: issue.parent_id,
        labels,
        comments: comments
            .into_iter()
            .map(|c| ExportedComment {
                content: c.content,
                created_at: c.created_at.to_rfc3339(),
            })
            .collect(),
        created_at: issue.created_at.to_rfc3339(),
        updated_at: issue.updated_at.to_rfc3339(),
        closed_at: issue.closed_at.map(|dt| dt.to_rfc3339()),
    })
}

pub fn run_json(db: &Database, output_path: &Path) -> Result<()> {
    let issues = db.list_issues(Some("all"), None, None)?;

    let exported: Vec<ExportedIssue> = issues
        .iter()
        .map(|i| export_issue(db, i))
        .collect::<Result<Vec<_>>>()?;

    let data = ExportData {
        version: 1,
        exported_at: chrono::Utc::now().to_rfc3339(),
        issues: exported,
    };

    let json = serde_json::to_string_pretty(&data)?;
    fs::write(output_path, json).context("Failed to write export file")?;

    println!(
        "Exported {} issues to {}",
        data.issues.len(),
        output_path.display()
    );
    Ok(())
}

pub fn run_markdown(db: &Database, output_path: &Path) -> Result<()> {
    let issues = db.list_issues(Some("all"), None, None)?;
    let mut md = String::new();

    md.push_str("# Chainlink Issues Export\n\n");
    md.push_str(&format!(
        "Exported: {}\n\n",
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    ));

    // Group by status
    let open: Vec<_> = issues.iter().filter(|i| i.status == "open").collect();
    let closed: Vec<_> = issues.iter().filter(|i| i.status == "closed").collect();

    if !open.is_empty() {
        md.push_str("## Open Issues\n\n");
        for issue in &open {
            write_issue_md(&mut md, db, issue)?;
        }
    }

    if !closed.is_empty() {
        md.push_str("## Closed Issues\n\n");
        for issue in &closed {
            write_issue_md(&mut md, db, issue)?;
        }
    }

    fs::write(output_path, md).context("Failed to write export file")?;
    println!(
        "Exported {} issues to {}",
        issues.len(),
        output_path.display()
    );
    Ok(())
}

fn write_issue_md(md: &mut String, db: &Database, issue: &Issue) -> Result<()> {
    let checkbox = if issue.status == "closed" {
        "[x]"
    } else {
        "[ ]"
    };

    md.push_str(&format!(
        "### {} #{}: {}\n\n",
        checkbox, issue.id, issue.title
    ));
    md.push_str(&format!("- **Priority:** {}\n", issue.priority));
    md.push_str(&format!("- **Status:** {}\n", issue.status));

    if let Some(parent_id) = issue.parent_id {
        md.push_str(&format!("- **Parent:** #{}\n", parent_id));
    }

    let labels = db.get_labels(issue.id)?;
    if !labels.is_empty() {
        md.push_str(&format!("- **Labels:** {}\n", labels.join(", ")));
    }

    md.push_str(&format!(
        "- **Created:** {}\n",
        issue.created_at.format("%Y-%m-%d")
    ));

    if let Some(ref desc) = issue.description {
        if !desc.is_empty() {
            md.push_str(&format!("\n{}\n", desc));
        }
    }

    let comments = db.get_comments(issue.id)?;
    if !comments.is_empty() {
        md.push_str("\n**Comments:**\n");
        for comment in comments {
            md.push_str(&format!(
                "- [{}] {}\n",
                comment.created_at.format("%Y-%m-%d %H:%M"),
                comment.content
            ));
        }
    }

    md.push_str("\n---\n\n");
    Ok(())
}
