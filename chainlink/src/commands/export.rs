use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, Write};

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
    #[serde(default = "default_comment_kind")]
    pub kind: String,
}

fn default_comment_kind() -> String {
    "note".to_string()
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
                kind: c.kind,
            })
            .collect(),
        created_at: issue.created_at.to_rfc3339(),
        updated_at: issue.updated_at.to_rfc3339(),
        closed_at: issue.closed_at.map(|dt| dt.to_rfc3339()),
    })
}

pub fn run_json(db: &Database, output_path: Option<&str>) -> Result<()> {
    let issues = db.list_issues(Some("all"), &[], None)?;

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

    match output_path {
        Some(path) => {
            fs::write(path, json).context("Failed to write export file")?;
            eprintln!("Exported {} issues to {}", data.issues.len(), path);
        }
        None => {
            let mut stdout = io::stdout().lock();
            writeln!(stdout, "{}", json)?;
        }
    }
    Ok(())
}

pub fn run_markdown(db: &Database, output_path: Option<&str>) -> Result<()> {
    let issues = db.list_issues(Some("all"), &[], None)?;
    let mut md = String::new();

    md.push_str("# Chainlink Issues Export\n\n");
    md.push_str(&format!(
        "Exported: {}\n\n",
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    ));

    // Group by status
    let open: Vec<_> = issues.iter().filter(|i| i.status == "open").collect();
    let closed: Vec<_> = issues.iter().filter(|i| i.status == "closed").collect();
    let archived: Vec<_> = issues.iter().filter(|i| i.status == "archived").collect();

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

    if !archived.is_empty() {
        md.push_str("## Archived Issues\n\n");
        for issue in &archived {
            write_issue_md(&mut md, db, issue)?;
        }
    }

    match output_path {
        Some(path) => {
            fs::write(path, md).context("Failed to write export file")?;
            eprintln!("Exported {} issues to {}", issues.len(), path);
        }
        None => {
            let mut stdout = io::stdout().lock();
            writeln!(stdout, "{}", md)?;
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tempfile::tempdir;

    fn setup_test_db() -> (Database, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Database::open(&db_path).unwrap();
        (db, dir)
    }

    #[test]
    fn test_export_issue_basic() {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test issue", None, "medium").unwrap();
        let issue = db.get_issue(id).unwrap().unwrap();
        let exported = export_issue(&db, &issue).unwrap();
        assert_eq!(exported.id, id);
        assert_eq!(exported.title, "Test issue");
        assert_eq!(exported.priority, "medium");
        assert_eq!(exported.status, "open");
    }

    #[test]
    fn test_export_issue_with_labels() {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test issue", None, "medium").unwrap();
        db.add_label(id, "bug").unwrap();
        db.add_label(id, "urgent").unwrap();
        let issue = db.get_issue(id).unwrap().unwrap();
        let exported = export_issue(&db, &issue).unwrap();
        assert_eq!(exported.labels.len(), 2);
    }

    #[test]
    fn test_export_issue_with_comments() {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test issue", None, "medium").unwrap();
        db.add_comment(id, "First comment", "note").unwrap();
        db.add_comment(id, "Second comment", "note").unwrap();
        let issue = db.get_issue(id).unwrap().unwrap();
        let exported = export_issue(&db, &issue).unwrap();
        assert_eq!(exported.comments.len(), 2);
    }

    #[test]
    fn test_export_closed_issue() {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test issue", None, "medium").unwrap();
        db.close_issue(id).unwrap();
        let issue = db.get_issue(id).unwrap().unwrap();
        let exported = export_issue(&db, &issue).unwrap();
        assert_eq!(exported.status, "closed");
        assert!(exported.closed_at.is_some());
    }

    #[test]
    fn test_run_json_to_file() {
        let (db, dir) = setup_test_db();
        db.create_issue("Issue 1", None, "high").unwrap();
        db.create_issue("Issue 2", Some("Description"), "low")
            .unwrap();
        let output_path = dir.path().join("export.json");
        let result = run_json(&db, Some(output_path.to_str().unwrap()));
        assert!(result.is_ok());
        let content = fs::read_to_string(&output_path).unwrap();
        let data: ExportData = serde_json::from_str(&content).unwrap();
        assert_eq!(data.version, 1);
        assert_eq!(data.issues.len(), 2);
    }

    #[test]
    fn test_run_json_empty_database() {
        let (db, dir) = setup_test_db();
        let output_path = dir.path().join("export.json");
        let result = run_json(&db, Some(output_path.to_str().unwrap()));
        assert!(result.is_ok());
        let content = fs::read_to_string(&output_path).unwrap();
        let data: ExportData = serde_json::from_str(&content).unwrap();
        assert_eq!(data.issues.len(), 0);
    }

    #[test]
    fn test_run_markdown_to_file() {
        let (db, dir) = setup_test_db();
        db.create_issue("Issue 1", None, "high").unwrap();
        let output_path = dir.path().join("export.md");
        let result = run_markdown(&db, Some(output_path.to_str().unwrap()));
        assert!(result.is_ok());
        let content = fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("# Chainlink Issues Export"));
    }

    #[test]
    fn test_markdown_groups_by_status() {
        let (db, dir) = setup_test_db();
        db.create_issue("Open issue", None, "medium").unwrap();
        let closed_id = db.create_issue("Closed issue", None, "medium").unwrap();
        db.close_issue(closed_id).unwrap();
        let output_path = dir.path().join("export.md");
        run_markdown(&db, Some(output_path.to_str().unwrap())).unwrap();
        let content = fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("## Open Issues"));
        assert!(content.contains("## Closed Issues"));
    }

    #[test]
    fn test_export_unicode_content() {
        let (db, dir) = setup_test_db();
        let id = db
            .create_issue("Test 🐛", Some("Description αβγ"), "medium")
            .unwrap();
        db.add_label(id, "バグ").unwrap();
        let output_path = dir.path().join("export.json");
        run_json(&db, Some(output_path.to_str().unwrap())).unwrap();
        let content = fs::read_to_string(&output_path).unwrap();
        let data: ExportData = serde_json::from_str(&content).unwrap();
        assert_eq!(data.issues[0].title, "Test 🐛");
    }

    #[test]
    fn test_export_data_roundtrip() {
        let data = ExportData {
            version: 1,
            exported_at: "2024-01-01T00:00:00Z".to_string(),
            issues: vec![ExportedIssue {
                id: 1,
                title: "Test".to_string(),
                description: Some("Desc".to_string()),
                status: "open".to_string(),
                priority: "medium".to_string(),
                parent_id: None,
                labels: vec!["bug".to_string()],
                comments: vec![ExportedComment {
                    content: "Comment".to_string(),
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    kind: "note".to_string(),
                }],
                created_at: "2024-01-01T00:00:00Z".to_string(),
                updated_at: "2024-01-01T00:00:00Z".to_string(),
                closed_at: None,
            }],
        };
        let json = serde_json::to_string(&data).unwrap();
        let parsed: ExportData = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.version, data.version);
        assert_eq!(parsed.issues.len(), 1);
    }

    proptest! {
        #[test]
        fn prop_export_never_panics(title in "[a-zA-Z0-9 ]{1,50}") {
            let (db, dir) = setup_test_db();
            db.create_issue(&title, None, "medium").unwrap();
            let output_path = dir.path().join("export.json");
            let result = run_json(&db, Some(output_path.to_str().unwrap()));
            prop_assert!(result.is_ok());
        }

        #[test]
        fn prop_json_is_valid(title in "[a-zA-Z0-9 ]{1,30}") {
            let (db, dir) = setup_test_db();
            db.create_issue(&title, None, "medium").unwrap();
            let output_path = dir.path().join("export.json");
            run_json(&db, Some(output_path.to_str().unwrap())).unwrap();
            let content = fs::read_to_string(&output_path).unwrap();
            let result: Result<ExportData, _> = serde_json::from_str(&content);
            prop_assert!(result.is_ok());
        }
    }
}
