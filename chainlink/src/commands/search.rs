use anyhow::Result;
use serde_json;

use crate::db::Database;
use crate::utils::format_issue_id;

pub fn run_json(db: &Database, query: &str) -> Result<()> {
    let results = db.search_issues(query)?;
    println!("{}", serde_json::to_string_pretty(&results)?);
    Ok(())
}

pub fn run(db: &Database, query: &str) -> Result<()> {
    let results = db.search_issues(query)?;

    if results.is_empty() {
        println!("No issues found matching '{}'", query);
        return Ok(());
    }

    println!("Found {} issue(s) matching '{}':\n", results.len(), query);

    for issue in results {
        let status_marker = if issue.status == "closed" { "✓" } else { " " };
        let parent_str = issue
            .parent_id
            .map(|p| format!(" (sub of {})", format_issue_id(p)))
            .unwrap_or_default();

        println!(
            "{:<5} [{}] {:8} {}{} {}",
            format_issue_id(issue.id),
            status_marker,
            issue.priority,
            issue.title,
            parent_str,
            if issue.status == "closed" {
                "(closed)"
            } else {
                ""
            }
        );

        // Show snippet of description if it contains the query
        if let Some(ref desc) = issue.description {
            if desc.to_lowercase().contains(&query.to_lowercase()) {
                let preview: String = desc.chars().take(60).collect();
                let suffix = if desc.chars().count() > 60 { "..." } else { "" };
                println!("      └─ {}{}", preview.replace('\n', " "), suffix);
            }
        }
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

    // ==================== Unit Tests ====================

    #[test]
    fn test_search_finds_by_title() {
        let (db, _dir) = setup_test_db();
        let id = db
            .create_issue("Fix authentication bug", None, "high")
            .unwrap();
        db.create_issue("Add dark mode", None, "medium").unwrap();

        run(&db, "authentication").unwrap();
        let results = db.search_issues("authentication").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, id);
    }

    #[test]
    fn test_search_finds_by_description() {
        let (db, _dir) = setup_test_db();
        let id = db
            .create_issue("Feature A", Some("This relates to user login"), "medium")
            .unwrap();

        run(&db, "login").unwrap();
        let results = db.search_issues("login").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, id);
    }

    #[test]
    fn test_search_case_insensitive() {
        let (db, _dir) = setup_test_db();
        let id = db
            .create_issue("Fix AUTHENTICATION Bug", None, "high")
            .unwrap();

        run(&db, "authentication").unwrap();
        let results = db.search_issues("authentication").unwrap();
        assert_eq!(
            results.len(),
            1,
            "Case-insensitive search should find the issue"
        );
        assert_eq!(results[0].id, id);
    }

    #[test]
    fn test_search_no_results() {
        let (db, _dir) = setup_test_db();
        db.create_issue("Some issue", None, "medium").unwrap();

        run(&db, "nonexistent").unwrap();
        let results = db.search_issues("nonexistent").unwrap();
        assert!(
            results.is_empty(),
            "Search for nonexistent term should return empty"
        );
    }

    #[test]
    fn test_search_empty_database() {
        let (db, _dir) = setup_test_db();

        run(&db, "anything").unwrap();
        let results = db.search_issues("anything").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_empty_query() {
        let (db, _dir) = setup_test_db();
        db.create_issue("Test issue", None, "medium").unwrap();

        run(&db, "").unwrap();
        let _results = db.search_issues("").unwrap();
        // Empty query behavior: may match all or none depending on implementation
        // Just verify it doesn't error
    }

    #[test]
    fn test_search_special_characters() {
        let (db, _dir) = setup_test_db();
        let id = db
            .create_issue("Fix bug with @mentions", None, "medium")
            .unwrap();

        run(&db, "@mentions").unwrap();
        let results = db.search_issues("@mentions").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, id);
    }

    #[test]
    fn test_search_unicode() {
        let (db, _dir) = setup_test_db();
        let id = db
            .create_issue("Fix 日本語 support", None, "medium")
            .unwrap();

        run(&db, "日本語").unwrap();
        let results = db.search_issues("日本語").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, id);
    }

    #[test]
    fn test_search_sql_injection() {
        let (db, _dir) = setup_test_db();
        db.create_issue("Normal issue", None, "medium").unwrap();

        run(&db, "'; DROP TABLE issues; --").unwrap();
        let issues = db.list_issues(None, &[], None).unwrap();
        assert_eq!(
            issues.len(),
            1,
            "Database should be intact after SQL injection attempt"
        );
    }

    #[test]
    fn test_search_with_wildcards() {
        let (db, _dir) = setup_test_db();
        db.create_issue("Test issue with pattern", None, "medium")
            .unwrap();

        run(&db, "%pattern%").unwrap();
        let results = db.search_issues("%pattern%").unwrap();
        // SQL wildcards should be escaped -- literal "%pattern%" should NOT match "pattern"
        assert!(
            results.is_empty(),
            "Literal SQL wildcards should be escaped and not match"
        );
    }

    #[test]
    fn test_search_finds_in_comments() {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Generic issue", None, "medium").unwrap();
        db.add_comment(id, "Found the root cause in authentication module", "note")
            .unwrap();

        run(&db, "authentication").unwrap();
        let results = db.search_issues("authentication").unwrap();
        assert_eq!(
            results.len(),
            1,
            "Search should find issues via comment content"
        );
        assert_eq!(results[0].id, id);
    }

    #[test]
    fn test_search_subissue_shows_parent() {
        let (db, _dir) = setup_test_db();
        let parent_id = db.create_issue("Parent feature", None, "high").unwrap();
        let sub_id = db
            .create_subissue(parent_id, "Sub task authentication", None, "medium")
            .unwrap();

        run(&db, "authentication").unwrap();
        let results = db.search_issues("authentication").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, sub_id);
        assert_eq!(results[0].parent_id, Some(parent_id));
    }

    #[test]
    fn test_search_closed_issue() {
        let (db, _dir) = setup_test_db();
        let id = db
            .create_issue("Fix authentication bug", None, "high")
            .unwrap();
        db.close_issue(id).unwrap();

        run(&db, "authentication").unwrap();
        let results = db.search_issues("authentication").unwrap();
        assert_eq!(results.len(), 1, "Search should find closed issues too");
        assert_eq!(results[0].status, "closed");
    }

    // ==================== Property-Based Tests ====================

    proptest! {
        #[test]
        fn prop_search_never_panics(query in ".*") {
            let (db, _dir) = setup_test_db();
            db.create_issue("Test issue", None, "medium").unwrap();
            let _ = run(&db, &query);
        }

        #[test]
        fn prop_search_with_issues_never_panics(
            title in "[a-zA-Z0-9 ]{1,50}",
            query in "[a-zA-Z0-9]{1,20}"
        ) {
            let (db, _dir) = setup_test_db();
            db.create_issue(&title, None, "medium").unwrap();
            let result = run(&db, &query);
            prop_assert!(result.is_ok());
        }

        #[test]
        fn prop_search_unicode_never_panics(
            title in "[\\p{L}\\p{N} ]{1,30}",
            query in "[\\p{L}\\p{N}]{1,10}"
        ) {
            let (db, _dir) = setup_test_db();
            db.create_issue(&title, None, "medium").unwrap();
            let result = run(&db, &query);
            prop_assert!(result.is_ok());
        }
    }
}
