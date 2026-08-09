use super::*;
use proptest::prelude::*;

fn setup_test_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("issues.db");
    let db = Database::open(&db_path).unwrap();
    (db, dir)
}

// Generate valid priority strings
fn valid_priority() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("low".to_string()),
        Just("medium".to_string()),
        Just("high".to_string()),
        Just("critical".to_string()),
    ]
}

// Generate arbitrary (but safe) strings for titles
fn safe_string() -> impl Strategy<Value = String> {
    // Avoid null bytes; limit to MAX_TITLE_LEN so strings are valid as titles
    "[a-zA-Z0-9 _\\-\\.!?]{0,512}".prop_map(|s| s)
}

proptest! {
    /// Any valid title should be storable and retrievable unchanged
    #[test]
    fn prop_title_roundtrip(title in safe_string()) {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue(&title, None, "medium").unwrap();
        let issue = db.get_issue(id).unwrap().unwrap();
        prop_assert_eq!(issue.title, title);
    }

    /// Any valid description should be storable and retrievable unchanged
    #[test]
    fn prop_description_roundtrip(desc in safe_string()) {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test", Some(&desc), "medium").unwrap();
        let issue = db.get_issue(id).unwrap().unwrap();
        prop_assert_eq!(issue.description, Some(desc));
    }

    /// All valid priorities should work
    #[test]
    fn prop_priority_valid(priority in valid_priority()) {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test", None, &priority).unwrap();
        let issue = db.get_issue(id).unwrap().unwrap();
        prop_assert_eq!(issue.priority, priority);
    }

    /// Labels should be storable and retrievable
    #[test]
    fn prop_label_roundtrip(label in "[a-zA-Z0-9_\\-]{1,50}") {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test", None, "medium").unwrap();
        db.add_label(id, &label).unwrap();
        let labels = db.get_labels(id).unwrap();
        prop_assert!(labels.contains(&label));
    }

    /// Comments should be storable and retrievable
    #[test]
    fn prop_comment_roundtrip(content in safe_string()) {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test", None, "medium").unwrap();
        db.add_comment(id, &content, "note").unwrap();
        let comments = db.get_comments(id).unwrap();
        prop_assert_eq!(comments.len(), 1);
        prop_assert_eq!(&comments[0].content, &content);
    }

    /// Creating multiple issues should always increase count
    #[test]
    fn prop_create_increases_count(count in 1usize..20) {
        let (db, _dir) = setup_test_db();
        for i in 0..count {
            db.create_issue(&format!("Issue {}", i), None, "medium").unwrap();
        }
        let issues = db.list_issues(None, &[], None).unwrap();
        prop_assert_eq!(issues.len(), count);
    }

    /// Close then reopen should leave issue open
    #[test]
    fn prop_close_reopen_idempotent(title in safe_string()) {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue(&title, None, "medium").unwrap();

        db.close_issue(id).unwrap();
        let issue = db.get_issue(id).unwrap().unwrap();
        prop_assert_eq!(issue.status, "closed");

        db.reopen_issue(id).unwrap();
        let issue = db.get_issue(id).unwrap().unwrap();
        prop_assert_eq!(issue.status, "open");
    }

    /// Blocking should be reflected in blocked list
    #[test]
    fn prop_blocking_relationship(a in 1i64..100, b in 1i64..100) {
        if a == b {
            return Ok(()); // Skip self-blocking
        }
        let (db, _dir) = setup_test_db();

        // Create both issues
        for i in 1..=std::cmp::max(a, b) {
            db.create_issue(&format!("Issue {}", i), None, "medium").unwrap();
        }

        db.add_dependency(a, b).unwrap();
        let blockers = db.get_blockers(a).unwrap();
        prop_assert!(blockers.contains(&b));
    }

    /// Search should find issues with matching titles
    #[test]
    fn prop_search_finds_title(
        prefix in "[a-zA-Z]{3,10}",
        suffix in "[a-zA-Z]{3,10}"
    ) {
        let (db, _dir) = setup_test_db();
        let title = format!("{} unique marker {}", prefix, suffix);
        db.create_issue(&title, None, "medium").unwrap();

        // Search for the unique marker
        let results = db.search_issues("unique marker").unwrap();
        prop_assert!(!results.is_empty());
        prop_assert!(results.iter().any(|i| i.title.contains("unique marker")));
    }

    /// Circular dependencies should be prevented
    #[test]
    fn prop_no_circular_deps(chain_len in 2usize..6) {
        let (db, _dir) = setup_test_db();

        // Create a chain of issues
        let mut ids = Vec::new();
        for i in 0..chain_len {
            let id = db.create_issue(&format!("Issue {}", i), None, "medium").unwrap();
            ids.push(id);
        }

        // Create a linear dependency chain: 0 <- 1 <- 2 <- ... <- n-1
        for i in 0..chain_len - 1 {
            db.add_dependency(ids[i], ids[i + 1]).unwrap();
        }

        // Trying to close the cycle (n-1 <- 0) should fail
        let result = db.add_dependency(ids[chain_len - 1], ids[0]);
        prop_assert!(result.is_err(), "Circular dependency should be rejected");
    }

    /// Deleting a parent should cascade to all children
    #[test]
    fn prop_cascade_deletes_children(child_count in 1usize..5) {
        let (db, _dir) = setup_test_db();

        // Create parent
        let parent_id = db.create_issue("Parent", None, "medium").unwrap();

        // Create children
        let mut child_ids = Vec::new();
        for i in 0..child_count {
            let id = db.create_subissue(parent_id, &format!("Child {}", i), None, "low").unwrap();
            child_ids.push(id);
        }

        // Verify children exist
        let issues_before = db.list_issues(None, &[], None).unwrap();
        prop_assert_eq!(issues_before.len(), child_count + 1);

        // Delete parent
        db.delete_issue(parent_id).unwrap();

        // All children should be gone too
        let issues_after = db.list_issues(None, &[], None).unwrap();
        prop_assert_eq!(issues_after.len(), 0);

        // Verify each child is gone
        for child_id in child_ids {
            let child = db.get_issue(child_id).unwrap();
            prop_assert!(child.is_none(), "Child should be deleted");
        }
    }

    /// Ready list should never contain issues with open blockers
    #[test]
    fn prop_ready_list_correctness(issue_count in 2usize..8) {
        let (db, _dir) = setup_test_db();

        // Create issues
        let mut ids = Vec::new();
        for i in 0..issue_count {
            let id = db.create_issue(&format!("Issue {}", i), None, "medium").unwrap();
            ids.push(id);
        }

        // Create some dependencies (each issue blocked by next, except last)
        for i in 0..issue_count - 1 {
            let _ = db.add_dependency(ids[i], ids[i + 1]);
        }

        // Get ready issues
        let ready = db.list_ready_issues().unwrap();

        // Verify: no ready issue should have open blockers
        for issue in &ready {
            let blockers = db.get_blockers(issue.id).unwrap();
            for blocker_id in blockers {
                if let Some(blocker) = db.get_issue(blocker_id).unwrap() {
                    prop_assert_ne!(
                        blocker.status, "open",
                        "Ready issue {} has open blocker {}",
                        issue.id, blocker_id
                    );
                }
            }
        }
    }

    /// Session active_issue_id should be set to NULL when issue is deleted
    #[test]
    fn prop_session_issue_delete_cascade(title in safe_string()) {
        let (db, _dir) = setup_test_db();

        // Create issue and session
        let issue_id = db.create_issue(&title, None, "medium").unwrap();
        let session_id = db.start_session().unwrap();
        db.set_session_issue(session_id, issue_id).unwrap();

        // Verify session has issue
        let session = db.get_current_session().unwrap().unwrap();
        prop_assert_eq!(session.active_issue_id, Some(issue_id));

        // Delete the issue
        db.delete_issue(issue_id).unwrap();

        // Session should still exist but with NULL active_issue_id
        let session_after = db.get_current_session().unwrap().unwrap();
        prop_assert_eq!(session_after.id, session_id);
        prop_assert_eq!(session_after.active_issue_id, None, "Session active_issue_id should be NULL after issue deletion");
    }

    /// Search wildcards should be escaped properly
    #[test]
    fn prop_search_wildcards_escaped(
        prefix in "[a-zA-Z]{3,5}",
        suffix in "[a-zA-Z]{3,5}"
    ) {
        let (db, _dir) = setup_test_db();

        // Create an issue with % and _ in title
        let special_title = format!("{}%test_marker{}", prefix, suffix);
        db.create_issue(&special_title, None, "medium").unwrap();

        // Create another issue that would match if wildcards weren't escaped
        db.create_issue("other content here", None, "medium").unwrap();

        // Search for the special characters literally
        let results = db.search_issues("%test_").unwrap();

        // Should find only the issue with literal % and _
        prop_assert!(results.iter().all(|i| i.title.contains("%test_")));
    }
}
