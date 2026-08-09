use anyhow::{bail, Context, Result};
use std::fs;
use std::path::Path;

use crate::db::Database;
use crate::utils::format_issue_id;

pub fn close(db: &Database, id: i64, update_changelog: bool, chainlink_dir: &Path) -> Result<()> {
    close_inner(db, id, update_changelog, chainlink_dir, false)
}

pub fn close_quiet(
    db: &Database,
    id: i64,
    update_changelog: bool,
    chainlink_dir: &Path,
) -> Result<()> {
    close_inner(db, id, update_changelog, chainlink_dir, true)
}

fn close_inner(
    db: &Database,
    id: i64,
    update_changelog: bool,
    chainlink_dir: &Path,
    quiet: bool,
) -> Result<()> {
    // Get issue details before closing
    let issue = db.get_issue(id)?;
    let issue = match issue {
        Some(i) => i,
        None => bail!("Issue {} not found", format_issue_id(id)),
    };
    let labels = db.get_labels(id)?;

    if db.close_issue(id)? {
        if !quiet {
            println!("Closed issue {}", format_issue_id(id));
        }
    } else {
        bail!("Issue {} not found", format_issue_id(id));
    }

    // Update changelog if requested
    if update_changelog {
        let project_root = chainlink_dir.parent().unwrap_or(chainlink_dir);
        let changelog_path = project_root.join("CHANGELOG.md");

        // Create CHANGELOG.md if it doesn't exist
        if !changelog_path.exists() {
            if let Err(e) = create_changelog(&changelog_path) {
                tracing::warn!("Could not create CHANGELOG.md: {}", e);
            } else {
                println!("Created CHANGELOG.md");
            }
        }

        if changelog_path.exists() {
            let category = determine_changelog_category(&labels);
            let entry = format!("- {} ({})\n", issue.title, format_issue_id(id));

            if let Err(e) = append_to_changelog(&changelog_path, &category, &entry) {
                tracing::warn!("Could not update CHANGELOG.md: {}", e);
            } else if !quiet {
                println!("Added to CHANGELOG.md under {}", category);
            }
        }
    }

    Ok(())
}

fn create_changelog(path: &Path) -> Result<()> {
    let template = r#"# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added

### Fixed

### Changed
"#;
    fs::write(path, template).context("Failed to create CHANGELOG.md")?;
    Ok(())
}

fn determine_changelog_category(labels: &[String]) -> String {
    for label in labels {
        match label.to_lowercase().as_str() {
            "bug" | "fix" | "bugfix" => return "Fixed".to_string(),
            "feature" | "enhancement" => return "Added".to_string(),
            "breaking" | "breaking-change" => return "Changed".to_string(),
            "deprecated" => return "Deprecated".to_string(),
            "removed" => return "Removed".to_string(),
            "security" => return "Security".to_string(),
            _ => continue,
        }
    }
    "Changed".to_string() // Default category
}

fn append_to_changelog(path: &Path, category: &str, entry: &str) -> Result<()> {
    let content = fs::read_to_string(path).context("Failed to read CHANGELOG.md")?;
    let heading = format!("### {}", category);

    let new_content = if content.contains(&heading) {
        // Insert after the heading
        let mut result = String::new();
        let mut found = false;
        for line in content.lines() {
            result.push_str(line);
            result.push('\n');
            if !found && line.trim() == heading {
                result.push_str(entry);
                found = true;
            }
        }
        result
    } else {
        // Add new section after first ## heading (usually ## [Unreleased])
        let mut result = String::new();
        let mut added = false;
        for line in content.lines() {
            result.push_str(line);
            result.push('\n');
            if !added && line.starts_with("## ") {
                result.push('\n');
                result.push_str(&format!("{}\n", heading));
                result.push_str(entry);
                added = true;
            }
        }
        if !added {
            // No ## heading found, append at end
            result.push_str(&format!("\n{}\n", heading));
            result.push_str(entry);
        }
        result
    };

    fs::write(path, new_content).context("Failed to write CHANGELOG.md")?;
    Ok(())
}

pub fn close_all(
    db: &Database,
    label_filters: &[String],
    priority_filter: Option<&str>,
    update_changelog: bool,
    chainlink_dir: &Path,
) -> Result<()> {
    let issues = db.list_issues(Some("open"), label_filters, priority_filter)?;

    if issues.is_empty() {
        println!("No matching open issues found.");
        return Ok(());
    }

    let mut closed_count = 0;
    for issue in &issues {
        match close(db, issue.id, update_changelog, chainlink_dir) {
            Ok(()) => closed_count += 1,
            Err(e) => tracing::warn!("Failed to close {}: {}", format_issue_id(issue.id), e),
        }
    }

    println!("Closed {} issue(s).", closed_count);
    Ok(())
}

pub fn reopen(db: &Database, id: i64) -> Result<()> {
    if db.reopen_issue(id)? {
        println!("Reopened issue {}", format_issue_id(id));
    } else {
        bail!("Issue {} not found", format_issue_id(id));
    }
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

    // ==================== Close Tests ====================

    #[test]
    fn test_close_existing_issue() {
        let (db, _dir) = setup_test_db();
        let chainlink_dir = _dir.path().join(".chainlink");
        std::fs::create_dir_all(&chainlink_dir).unwrap();

        let issue_id = db.create_issue("Test issue", None, "medium").unwrap();

        let result = close(&db, issue_id, false, &chainlink_dir);
        assert!(result.is_ok());

        let issue = db.get_issue(issue_id).unwrap().unwrap();
        assert_eq!(issue.status, "closed");
        assert!(issue.closed_at.is_some());
    }

    #[test]
    fn test_close_nonexistent_issue() {
        let (db, _dir) = setup_test_db();
        let chainlink_dir = _dir.path().join(".chainlink");
        std::fs::create_dir_all(&chainlink_dir).unwrap();

        let result = close(&db, 99999, false, &chainlink_dir);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_close_already_closed_issue() {
        let (db, _dir) = setup_test_db();
        let chainlink_dir = _dir.path().join(".chainlink");
        std::fs::create_dir_all(&chainlink_dir).unwrap();

        let issue_id = db.create_issue("Test issue", None, "medium").unwrap();
        db.close_issue(issue_id).unwrap();

        // Closing again should be fine (idempotent at db level)
        let result = close(&db, issue_id, false, &chainlink_dir);
        assert!(result.is_ok());
    }

    // ==================== Reopen Tests ====================

    #[test]
    fn test_reopen_closed_issue() {
        let (db, _dir) = setup_test_db();

        let issue_id = db.create_issue("Test issue", None, "medium").unwrap();
        db.close_issue(issue_id).unwrap();

        let result = reopen(&db, issue_id);
        assert!(result.is_ok());

        let issue = db.get_issue(issue_id).unwrap().unwrap();
        assert_eq!(issue.status, "open");
        assert!(issue.closed_at.is_none());
    }

    #[test]
    fn test_reopen_nonexistent_issue() {
        let (db, _dir) = setup_test_db();

        let result = reopen(&db, 99999);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_reopen_already_open_issue() {
        let (db, _dir) = setup_test_db();

        let issue_id = db.create_issue("Test issue", None, "medium").unwrap();

        // Reopening an open issue - succeeds (idempotent operation)
        let result = reopen(&db, issue_id);
        assert!(result.is_ok());

        let issue = db.get_issue(issue_id).unwrap().unwrap();
        assert_eq!(issue.status, "open");
    }

    // ==================== Changelog Category Tests ====================

    #[test]
    fn test_determine_changelog_category_bug() {
        assert_eq!(determine_changelog_category(&["bug".to_string()]), "Fixed");
        assert_eq!(determine_changelog_category(&["fix".to_string()]), "Fixed");
        assert_eq!(
            determine_changelog_category(&["bugfix".to_string()]),
            "Fixed"
        );
    }

    #[test]
    fn test_determine_changelog_category_feature() {
        assert_eq!(
            determine_changelog_category(&["feature".to_string()]),
            "Added"
        );
        assert_eq!(
            determine_changelog_category(&["enhancement".to_string()]),
            "Added"
        );
    }

    #[test]
    fn test_determine_changelog_category_breaking() {
        assert_eq!(
            determine_changelog_category(&["breaking".to_string()]),
            "Changed"
        );
        assert_eq!(
            determine_changelog_category(&["breaking-change".to_string()]),
            "Changed"
        );
    }

    #[test]
    fn test_determine_changelog_category_other() {
        assert_eq!(
            determine_changelog_category(&["deprecated".to_string()]),
            "Deprecated"
        );
        assert_eq!(
            determine_changelog_category(&["removed".to_string()]),
            "Removed"
        );
        assert_eq!(
            determine_changelog_category(&["security".to_string()]),
            "Security"
        );
    }

    #[test]
    fn test_determine_changelog_category_default() {
        assert_eq!(
            determine_changelog_category(&["unknown".to_string()]),
            "Changed"
        );
        assert_eq!(determine_changelog_category(&[]), "Changed");
    }

    #[test]
    fn test_determine_changelog_category_first_match_wins() {
        // Bug comes before feature, so Fixed should win
        assert_eq!(
            determine_changelog_category(&["bug".to_string(), "feature".to_string()]),
            "Fixed"
        );
    }

    #[test]
    fn test_determine_changelog_category_case_insensitive() {
        assert_eq!(determine_changelog_category(&["BUG".to_string()]), "Fixed");
        assert_eq!(
            determine_changelog_category(&["Feature".to_string()]),
            "Added"
        );
    }

    // ==================== Close/Reopen Cycle Tests ====================

    #[test]
    fn test_close_reopen_cycle() {
        let (db, _dir) = setup_test_db();
        let chainlink_dir = _dir.path().join(".chainlink");
        std::fs::create_dir_all(&chainlink_dir).unwrap();

        let issue_id = db.create_issue("Test issue", None, "medium").unwrap();

        // Close
        close(&db, issue_id, false, &chainlink_dir).unwrap();
        let issue = db.get_issue(issue_id).unwrap().unwrap();
        assert_eq!(issue.status, "closed");

        // Reopen
        reopen(&db, issue_id).unwrap();
        let issue = db.get_issue(issue_id).unwrap().unwrap();
        assert_eq!(issue.status, "open");

        // Close again
        close(&db, issue_id, false, &chainlink_dir).unwrap();
        let issue = db.get_issue(issue_id).unwrap().unwrap();
        assert_eq!(issue.status, "closed");
    }

    // ==================== Property-Based Tests ====================

    proptest! {
        #[test]
        fn prop_close_sets_status_to_closed(title in "[a-zA-Z0-9 ]{1,50}") {
            let (db, _dir) = setup_test_db();
            let chainlink_dir = _dir.path().join(".chainlink");
            std::fs::create_dir_all(&chainlink_dir).unwrap();

            let issue_id = db.create_issue(&title, None, "medium").unwrap();
            close(&db, issue_id, false, &chainlink_dir).unwrap();

            let issue = db.get_issue(issue_id).unwrap().unwrap();
            prop_assert_eq!(issue.status, "closed");
        }

        #[test]
        fn prop_reopen_sets_status_to_open(title in "[a-zA-Z0-9 ]{1,50}") {
            let (db, _dir) = setup_test_db();

            let issue_id = db.create_issue(&title, None, "medium").unwrap();
            db.close_issue(issue_id).unwrap();

            reopen(&db, issue_id).unwrap();

            let issue = db.get_issue(issue_id).unwrap().unwrap();
            prop_assert_eq!(issue.status, "open");
        }

        #[test]
        fn prop_nonexistent_issue_close_fails(issue_id in 1000i64..10000) {
            let (db, _dir) = setup_test_db();
            let chainlink_dir = _dir.path().join(".chainlink");
            std::fs::create_dir_all(&chainlink_dir).unwrap();

            let result = close(&db, issue_id, false, &chainlink_dir);
            prop_assert!(result.is_err());
        }

        #[test]
        fn prop_nonexistent_issue_reopen_fails(issue_id in 1000i64..10000) {
            let (db, _dir) = setup_test_db();

            let result = reopen(&db, issue_id);
            prop_assert!(result.is_err());
        }

        #[test]
        fn prop_changelog_category_returns_known_category(
            labels in proptest::collection::vec("[a-zA-Z]{1,20}", 0..5)
        ) {
            let valid_categories = ["Fixed", "Added", "Changed", "Deprecated", "Removed", "Security"];
            let category = determine_changelog_category(&labels);
            prop_assert!(
                valid_categories.contains(&category.as_str()),
                "Got unknown category: {}", category
            );
        }
    }
}
