use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use super::export::{ExportData, ExportedIssue};
use crate::db::Database;
use crate::utils::format_issue_id;

pub fn run_json(db: &Database, input_path: &Path) -> Result<()> {
    let content = fs::read_to_string(input_path).context("Failed to read import file")?;

    let data: ExportData = serde_json::from_str(&content).context("Failed to parse JSON")?;

    println!(
        "Importing {} issues from {}",
        data.issues.len(),
        input_path.display()
    );

    // Wrap entire import in a transaction for atomicity
    // If any part fails, all changes are rolled back
    let count = db.transaction(|| {
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

        Ok(data.issues.len())
    })?;

    println!("Successfully imported {} issues", count);
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
        db.add_comment(id, &comment.content, &comment.kind)?;
    }

    // Close if needed
    if issue.status == "closed" {
        db.close_issue(id)?;
    }

    println!(
        "  Imported: {} -> {} {}",
        format_issue_id(issue.id),
        format_issue_id(id),
        issue.title
    );
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::super::export::{ExportData, ExportedIssue};
    use super::*;
    use proptest::prelude::*;
    use tempfile::tempdir;

    fn setup_test_db() -> (Database, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Database::open(&db_path).unwrap();
        (db, dir)
    }

    fn create_test_export(issues: Vec<ExportedIssue>) -> String {
        let data = ExportData {
            version: 1,
            exported_at: "2024-01-01T00:00:00Z".to_string(),
            issues,
        };
        serde_json::to_string_pretty(&data).unwrap()
    }

    fn make_issue(id: i64, title: &str, parent_id: Option<i64>, status: &str) -> ExportedIssue {
        ExportedIssue {
            id,
            title: title.to_string(),
            description: None,
            status: status.to_string(),
            priority: "medium".to_string(),
            parent_id,
            labels: vec![],
            comments: vec![],
            created_at: "2024-01-01T00:00:00Z".to_string(),
            updated_at: "2024-01-01T00:00:00Z".to_string(),
            closed_at: None,
        }
    }

    #[test]
    fn test_import_single_issue() {
        let (db, dir) = setup_test_db();
        let json = create_test_export(vec![make_issue(1, "Test issue", None, "open")]);
        let import_path = dir.path().join("import.json");
        fs::write(&import_path, json).unwrap();
        let result = run_json(&db, &import_path);
        assert!(result.is_ok());
        let issues = db.list_issues(Some("all"), &[], None).unwrap();
        assert_eq!(issues.len(), 1);
    }

    #[test]
    fn test_import_multiple_issues() {
        let (db, dir) = setup_test_db();
        let json = create_test_export(vec![
            make_issue(1, "Issue 1", None, "open"),
            make_issue(2, "Issue 2", None, "open"),
        ]);
        let import_path = dir.path().join("import.json");
        fs::write(&import_path, json).unwrap();
        run_json(&db, &import_path).unwrap();
        let issues = db.list_issues(Some("all"), &[], None).unwrap();
        assert_eq!(issues.len(), 2);
    }

    #[test]
    fn test_import_closed_issue() {
        let (db, dir) = setup_test_db();
        let json = create_test_export(vec![make_issue(1, "Closed", None, "closed")]);
        let import_path = dir.path().join("import.json");
        fs::write(&import_path, json).unwrap();
        run_json(&db, &import_path).unwrap();
        let issues = db.list_issues(Some("closed"), &[], None).unwrap();
        assert_eq!(issues.len(), 1);
    }

    #[test]
    fn test_import_with_labels() {
        let (db, dir) = setup_test_db();
        let mut issue = make_issue(1, "Labeled", None, "open");
        issue.labels = vec!["bug".to_string()];
        let json = create_test_export(vec![issue]);
        let import_path = dir.path().join("import.json");
        fs::write(&import_path, json).unwrap();
        run_json(&db, &import_path).unwrap();
        let issues = db.list_issues(Some("all"), &[], None).unwrap();
        let labels = db.get_labels(issues[0].id).unwrap();
        assert!(labels.contains(&"bug".to_string()));
    }

    #[test]
    fn test_import_invalid_json() {
        let (db, dir) = setup_test_db();
        let import_path = dir.path().join("invalid.json");
        fs::write(&import_path, "not valid json").unwrap();
        let result = run_json(&db, &import_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_import_missing_file() {
        let (db, dir) = setup_test_db();
        let import_path = dir.path().join("nonexistent.json");
        let result = run_json(&db, &import_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_import_empty_issues() {
        let (db, dir) = setup_test_db();
        let json = create_test_export(vec![]);
        let import_path = dir.path().join("import.json");
        fs::write(&import_path, json).unwrap();
        let result = run_json(&db, &import_path);
        assert!(result.is_ok());
    }

    proptest! {
        #[test]
        fn prop_import_never_panics(title in "[a-zA-Z0-9 ]{1,50}") {
            let (db, dir) = setup_test_db();
            let json = create_test_export(vec![make_issue(1, &title, None, "open")]);
            let import_path = dir.path().join("import.json");
            fs::write(&import_path, json).unwrap();
            let result = run_json(&db, &import_path);
            prop_assert!(result.is_ok());
        }
    }
}
