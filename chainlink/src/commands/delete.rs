use anyhow::{bail, Result};
use std::io::{self, Write};

use crate::db::Database;
use crate::utils::format_issue_id;

pub fn run(db: &Database, id: i64, force: bool) -> Result<()> {
    // Check if issue exists first
    let issue = match db.get_issue(id)? {
        Some(i) => i,
        None => bail!("Issue {} not found", format_issue_id(id)),
    };

    if !force {
        print!(
            "Delete issue {} \"{}\"? [y/N] ",
            format_issue_id(id),
            issue.title
        );
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Cancelled.");
            return Ok(());
        }
    }

    if db.delete_issue(id)? {
        println!("Deleted issue {}", format_issue_id(id));
    } else {
        bail!("Failed to delete issue {}", format_issue_id(id));
    }

    Ok(())
}

/// Internal function for testing without stdin interaction
#[cfg(test)]
pub fn run_force(db: &Database, id: i64) -> Result<()> {
    run(db, id, true)
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
    fn test_delete_existing_issue_force() {
        let (db, _dir) = setup_test_db();
        let issue_id = db.create_issue("To delete", None, "medium").unwrap();

        let result = run_force(&db, issue_id);
        assert!(result.is_ok());

        // Verify issue is deleted
        let issue = db.get_issue(issue_id).unwrap();
        assert!(issue.is_none());
    }

    #[test]
    fn test_delete_nonexistent_issue() {
        let (db, _dir) = setup_test_db();

        let result = run_force(&db, 99999);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_delete_cascades_labels() {
        let (db, _dir) = setup_test_db();
        let issue_id = db.create_issue("Test", None, "medium").unwrap();
        db.add_label(issue_id, "bug").unwrap();
        db.add_label(issue_id, "urgent").unwrap();

        run_force(&db, issue_id).unwrap();

        // Labels should be gone
        let labels = db.get_labels(issue_id).unwrap();
        assert!(labels.is_empty());
    }

    #[test]
    fn test_delete_cascades_comments() {
        let (db, _dir) = setup_test_db();
        let issue_id = db.create_issue("Test", None, "medium").unwrap();
        db.add_comment(issue_id, "Comment 1", "note").unwrap();
        db.add_comment(issue_id, "Comment 2", "note").unwrap();

        run_force(&db, issue_id).unwrap();

        // Comments should be gone
        let comments = db.get_comments(issue_id).unwrap();
        assert!(comments.is_empty());
    }

    #[test]
    fn test_delete_cascades_subissues() {
        let (db, _dir) = setup_test_db();
        let parent_id = db.create_issue("Parent", None, "high").unwrap();
        let child1 = db
            .create_subissue(parent_id, "Child 1", None, "medium")
            .unwrap();
        let child2 = db
            .create_subissue(parent_id, "Child 2", None, "low")
            .unwrap();

        run_force(&db, parent_id).unwrap();

        // All children should be deleted
        assert!(db.get_issue(child1).unwrap().is_none());
        assert!(db.get_issue(child2).unwrap().is_none());
    }

    #[test]
    fn test_delete_removes_dependencies() {
        let (db, _dir) = setup_test_db();
        let blocker = db.create_issue("Blocker", None, "high").unwrap();
        let blocked = db.create_issue("Blocked", None, "medium").unwrap();
        db.add_dependency(blocked, blocker).unwrap();

        // Delete the blocker
        run_force(&db, blocker).unwrap();

        // The blocked issue should no longer have this blocker
        let blockers = db.get_blockers(blocked).unwrap();
        assert!(!blockers.contains(&blocker));
    }

    #[test]
    fn test_delete_removes_relations() {
        let (db, _dir) = setup_test_db();
        let issue1 = db.create_issue("Issue 1", None, "medium").unwrap();
        let issue2 = db.create_issue("Issue 2", None, "medium").unwrap();
        db.add_relation(issue1, issue2).unwrap();

        // Delete issue1
        run_force(&db, issue1).unwrap();

        // issue2 should no longer have this relation
        let related = db.get_related_issues(issue2).unwrap();
        assert!(related.is_empty());
    }

    #[test]
    fn test_delete_closed_issue() {
        let (db, _dir) = setup_test_db();
        let issue_id = db.create_issue("Closed issue", None, "medium").unwrap();
        db.close_issue(issue_id).unwrap();

        let result = run_force(&db, issue_id);
        assert!(result.is_ok());

        assert!(db.get_issue(issue_id).unwrap().is_none());
    }

    #[test]
    fn test_delete_archived_issue() {
        let (db, _dir) = setup_test_db();
        let issue_id = db.create_issue("Archived issue", None, "medium").unwrap();
        db.close_issue(issue_id).unwrap();
        db.archive_issue(issue_id).unwrap();

        let result = run_force(&db, issue_id);
        assert!(result.is_ok());

        assert!(db.get_issue(issue_id).unwrap().is_none());
    }

    #[test]
    fn test_delete_issue_with_timer() {
        let (db, _dir) = setup_test_db();
        let issue_id = db.create_issue("Timed issue", None, "medium").unwrap();
        db.start_timer(issue_id).unwrap();

        let result = run_force(&db, issue_id);
        assert!(result.is_ok());

        assert!(db.get_issue(issue_id).unwrap().is_none());
    }

    #[test]
    fn test_delete_issue_in_milestone() {
        let (db, _dir) = setup_test_db();
        let issue_id = db.create_issue("Milestone issue", None, "medium").unwrap();
        let milestone_id = db.create_milestone("v1.0", None).unwrap();
        db.add_issue_to_milestone(milestone_id, issue_id).unwrap();

        let result = run_force(&db, issue_id);
        assert!(result.is_ok());

        // Milestone should still exist
        assert!(db.get_milestone(milestone_id).unwrap().is_some());
        // But issue should be removed from it
        let issues = db.get_milestone_issues(milestone_id).unwrap();
        assert!(issues.is_empty());
    }

    #[test]
    fn test_delete_multiple_issues() {
        let (db, _dir) = setup_test_db();
        let id1 = db.create_issue("Issue 1", None, "medium").unwrap();
        let id2 = db.create_issue("Issue 2", None, "medium").unwrap();
        let id3 = db.create_issue("Issue 3", None, "medium").unwrap();

        run_force(&db, id1).unwrap();
        run_force(&db, id2).unwrap();

        // Only id3 should remain
        let issues = db.list_issues(None, &[], None).unwrap();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].id, id3);
    }

    // ==================== Property-Based Tests ====================

    proptest! {
        #[test]
        fn prop_delete_force_removes_issue(title in "[a-zA-Z0-9 ]{1,50}") {
            let (db, _dir) = setup_test_db();
            let issue_id = db.create_issue(&title, None, "medium").unwrap();

            run_force(&db, issue_id).unwrap();

            let issue = db.get_issue(issue_id).unwrap();
            prop_assert!(issue.is_none());
        }

        #[test]
        fn prop_delete_nonexistent_fails(issue_id in 1000i64..10000) {
            let (db, _dir) = setup_test_db();

            let result = run_force(&db, issue_id);
            prop_assert!(result.is_err());
        }

        #[test]
        fn prop_delete_cascade_labels(
            labels in proptest::collection::vec("[a-zA-Z]{1,10}", 1..5)
        ) {
            let (db, _dir) = setup_test_db();
            let issue_id = db.create_issue("Test", None, "medium").unwrap();

            for label in &labels {
                db.add_label(issue_id, label).unwrap();
            }

            run_force(&db, issue_id).unwrap();

            let remaining_labels = db.get_labels(issue_id).unwrap();
            prop_assert!(remaining_labels.is_empty());
        }

        #[test]
        fn prop_delete_cascade_comments(count in 1usize..5) {
            let (db, _dir) = setup_test_db();
            let issue_id = db.create_issue("Test", None, "medium").unwrap();

            for i in 0..count {
                db.add_comment(issue_id, &format!("Comment {}", i), "note").unwrap();
            }

            run_force(&db, issue_id).unwrap();

            let remaining_comments = db.get_comments(issue_id).unwrap();
            prop_assert!(remaining_comments.is_empty());
        }
    }
}
