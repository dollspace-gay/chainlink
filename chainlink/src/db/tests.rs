use super::*;
use tempfile::tempdir;

fn setup_test_db() -> (Database, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let db = Database::open(&db_path).unwrap();
    (db, dir)
}

// ==================== Issue CRUD Tests ====================

#[test]
fn test_create_and_get_issue() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test issue", None, "medium").unwrap();
    assert!(id > 0);

    let issue = db.get_issue(id).unwrap().unwrap();
    assert_eq!(issue.id, id);
    assert_eq!(issue.title, "Test issue");
    assert_eq!(issue.description, None);
    assert_eq!(issue.status, "open");
    assert_eq!(issue.priority, "medium");
    assert_eq!(issue.parent_id, None);
    assert!(issue.closed_at.is_none());
}

#[test]
fn test_create_issue_with_description() {
    let (db, _dir) = setup_test_db();

    let id = db
        .create_issue("Test issue", Some("Detailed description"), "high")
        .unwrap();
    let issue = db.get_issue(id).unwrap().unwrap();

    assert_eq!(issue.title, "Test issue");
    assert_eq!(issue.description, Some("Detailed description".to_string()));
    assert_eq!(issue.priority, "high");
}

#[test]
fn test_create_subissue() {
    let (db, _dir) = setup_test_db();

    let parent_id = db.create_issue("Parent issue", None, "high").unwrap();
    let child_id = db
        .create_subissue(parent_id, "Child issue", None, "medium")
        .unwrap();

    let child = db.get_issue(child_id).unwrap().unwrap();
    assert_eq!(child.parent_id, Some(parent_id));

    let subissues = db.get_subissues(parent_id).unwrap();
    assert_eq!(subissues.len(), 1);
    assert_eq!(subissues[0].id, child_id);
}

#[test]
fn test_get_nonexistent_issue() {
    let (db, _dir) = setup_test_db();
    let issue = db.get_issue(99999).unwrap();
    assert!(issue.is_none());
}

#[test]
fn test_list_issues() {
    let (db, _dir) = setup_test_db();

    db.create_issue("Issue 1", None, "low").unwrap();
    db.create_issue("Issue 2", None, "medium").unwrap();
    db.create_issue("Issue 3", None, "high").unwrap();

    let issues = db.list_issues(None, &[], None).unwrap();
    assert_eq!(issues.len(), 3);
}

#[test]
fn test_list_issues_filter_by_status() {
    let (db, _dir) = setup_test_db();

    let id1 = db.create_issue("Open issue", None, "low").unwrap();
    let id2 = db.create_issue("To be closed", None, "medium").unwrap();
    db.close_issue(id2).unwrap();

    let open_issues = db.list_issues(Some("open"), &[], None).unwrap();
    assert_eq!(open_issues.len(), 1);
    assert_eq!(open_issues[0].id, id1);

    let closed_issues = db.list_issues(Some("closed"), &[], None).unwrap();
    assert_eq!(closed_issues.len(), 1);
    assert_eq!(closed_issues[0].id, id2);

    let all_issues = db.list_issues(Some("all"), &[], None).unwrap();
    assert_eq!(all_issues.len(), 2);
}

#[test]
fn test_list_issues_filter_by_priority() {
    let (db, _dir) = setup_test_db();

    db.create_issue("Low priority", None, "low").unwrap();
    db.create_issue("High priority", None, "high").unwrap();

    let high_issues = db.list_issues(None, &[], Some("high")).unwrap();
    assert_eq!(high_issues.len(), 1);
    assert_eq!(high_issues[0].priority, "high");
}

#[test]
fn test_update_issue() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Original title", None, "low").unwrap();

    let updated = db
        .update_issue(
            id,
            Some("Updated title"),
            Some("New description"),
            Some("critical"),
        )
        .unwrap();
    assert!(updated);

    let issue = db.get_issue(id).unwrap().unwrap();
    assert_eq!(issue.title, "Updated title");
    assert_eq!(issue.description, Some("New description".to_string()));
    assert_eq!(issue.priority, "critical");
}

#[test]
fn test_update_issue_partial() {
    let (db, _dir) = setup_test_db();

    let id = db
        .create_issue("Original title", Some("Original desc"), "low")
        .unwrap();

    db.update_issue(id, Some("New title"), None, None).unwrap();

    let issue = db.get_issue(id).unwrap().unwrap();
    assert_eq!(issue.title, "New title");
    assert_eq!(issue.description, Some("Original desc".to_string()));
    assert_eq!(issue.priority, "low");
}

#[test]
fn test_close_and_reopen_issue() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test issue", None, "medium").unwrap();

    let closed = db.close_issue(id).unwrap();
    assert!(closed);

    let issue = db.get_issue(id).unwrap().unwrap();
    assert_eq!(issue.status, "closed");
    assert!(issue.closed_at.is_some());

    let reopened = db.reopen_issue(id).unwrap();
    assert!(reopened);

    let issue = db.get_issue(id).unwrap().unwrap();
    assert_eq!(issue.status, "open");
    assert!(issue.closed_at.is_none());
}

#[test]
fn test_close_nonexistent_issue_returns_false() {
    let (db, _dir) = setup_test_db();

    // Closing an issue that doesn't exist should return false
    let closed = db.close_issue(99999).unwrap();
    assert!(
        !closed,
        "close_issue should return false for nonexistent issue"
    );
}

#[test]
fn test_reopen_nonexistent_issue_returns_false() {
    let (db, _dir) = setup_test_db();

    // Reopening an issue that doesn't exist should return false
    let reopened = db.reopen_issue(99999).unwrap();
    assert!(
        !reopened,
        "reopen_issue should return false for nonexistent issue"
    );
}

#[test]
fn test_delete_issue() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("To delete", None, "low").unwrap();
    assert!(db.get_issue(id).unwrap().is_some());

    let deleted = db.delete_issue(id).unwrap();
    assert!(deleted);
    assert!(db.get_issue(id).unwrap().is_none());
}

#[test]
fn test_delete_nonexistent_issue() {
    let (db, _dir) = setup_test_db();
    let deleted = db.delete_issue(99999).unwrap();
    assert!(!deleted);
}

// ==================== Labels Tests ====================

#[test]
fn test_add_and_get_labels() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test issue", None, "medium").unwrap();

    db.add_label(id, "bug").unwrap();
    db.add_label(id, "urgent").unwrap();

    let labels = db.get_labels(id).unwrap();
    assert_eq!(labels.len(), 2);
    assert!(labels.contains(&"bug".to_string()));
    assert!(labels.contains(&"urgent".to_string()));
}

#[test]
fn test_add_duplicate_label_returns_false() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test issue", None, "medium").unwrap();

    // First add should return true (label was added)
    let first = db.add_label(id, "bug").unwrap();
    assert!(first, "First add_label should return true");

    // Second add should return false (duplicate, nothing inserted)
    let second = db.add_label(id, "bug").unwrap();
    assert!(!second, "Duplicate add_label should return false");

    let labels = db.get_labels(id).unwrap();
    assert_eq!(labels.len(), 1);
}

#[test]
fn test_remove_label() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test issue", None, "medium").unwrap();

    db.add_label(id, "bug").unwrap();
    db.add_label(id, "urgent").unwrap();

    let removed = db.remove_label(id, "bug").unwrap();
    assert!(removed);

    let labels = db.get_labels(id).unwrap();
    assert_eq!(labels.len(), 1);
    assert_eq!(labels[0], "urgent");
}

#[test]
fn test_remove_nonexistent_label_returns_false() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test issue", None, "medium").unwrap();
    db.add_label(id, "bug").unwrap();

    // Removing a label that doesn't exist should return false
    let removed = db.remove_label(id, "nonexistent").unwrap();
    assert!(
        !removed,
        "remove_label should return false for nonexistent label"
    );
}

#[test]
fn test_list_issues_filter_by_label() {
    let (db, _dir) = setup_test_db();

    let id1 = db.create_issue("Bug issue", None, "high").unwrap();
    let id2 = db.create_issue("Feature issue", None, "medium").unwrap();

    db.add_label(id1, "bug").unwrap();
    db.add_label(id2, "feature").unwrap();

    let bug_issues = db.list_issues(None, &["bug".to_string()], None).unwrap();
    assert_eq!(bug_issues.len(), 1);
    assert_eq!(bug_issues[0].id, id1);
}

// ==================== Comments Tests ====================

#[test]
fn test_add_and_get_comments() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test issue", None, "medium").unwrap();

    let comment_id = db.add_comment(id, "First comment", "note").unwrap();
    assert!(comment_id > 0);

    db.add_comment(id, "Second comment", "note").unwrap();

    let comments = db.get_comments(id).unwrap();
    assert_eq!(comments.len(), 2);
    assert_eq!(comments[0].content, "First comment");
    assert_eq!(comments[1].content, "Second comment");
}

// ==================== Dependencies Tests ====================

#[test]
fn test_add_and_get_dependencies() {
    let (db, _dir) = setup_test_db();

    let blocker = db.create_issue("Blocker issue", None, "high").unwrap();
    let blocked = db.create_issue("Blocked issue", None, "medium").unwrap();

    db.add_dependency(blocked, blocker).unwrap();

    let blockers = db.get_blockers(blocked).unwrap();
    assert_eq!(blockers.len(), 1);
    assert_eq!(blockers[0], blocker);

    let blocking = db.get_blocking(blocker).unwrap();
    assert_eq!(blocking.len(), 1);
    assert_eq!(blocking[0], blocked);
}

#[test]
fn test_remove_dependency() {
    let (db, _dir) = setup_test_db();

    let blocker = db.create_issue("Blocker", None, "high").unwrap();
    let blocked = db.create_issue("Blocked", None, "medium").unwrap();

    db.add_dependency(blocked, blocker).unwrap();
    let removed = db.remove_dependency(blocked, blocker).unwrap();
    assert!(removed);

    let blockers = db.get_blockers(blocked).unwrap();
    assert!(blockers.is_empty());
}

#[test]
fn test_list_blocked_issues() {
    let (db, _dir) = setup_test_db();

    let blocker = db.create_issue("Blocker", None, "high").unwrap();
    let blocked = db.create_issue("Blocked", None, "medium").unwrap();
    let unblocked = db.create_issue("Unblocked", None, "low").unwrap();

    db.add_dependency(blocked, blocker).unwrap();

    let blocked_issues = db.list_blocked_issues().unwrap();
    assert_eq!(blocked_issues.len(), 1);
    assert_eq!(blocked_issues[0].id, blocked);

    // Unblocked issue should not appear
    assert!(!blocked_issues.iter().any(|i| i.id == unblocked));
}

#[test]
fn test_list_ready_issues() {
    let (db, _dir) = setup_test_db();

    let blocker = db.create_issue("Blocker", None, "high").unwrap();
    let blocked = db.create_issue("Blocked", None, "medium").unwrap();
    let ready = db.create_issue("Ready", None, "low").unwrap();

    db.add_dependency(blocked, blocker).unwrap();

    let ready_issues = db.list_ready_issues().unwrap();

    // Blocker and ready should be in ready list (not blocked by anything)
    let ready_ids: Vec<i64> = ready_issues.iter().map(|i| i.id).collect();
    assert!(ready_ids.contains(&blocker));
    assert!(ready_ids.contains(&ready));
    assert!(!ready_ids.contains(&blocked));
}

#[test]
fn test_blocked_becomes_ready_when_blocker_closed() {
    let (db, _dir) = setup_test_db();

    let blocker = db.create_issue("Blocker", None, "high").unwrap();
    let blocked = db.create_issue("Blocked", None, "medium").unwrap();

    db.add_dependency(blocked, blocker).unwrap();

    // Initially blocked
    let blocked_issues = db.list_blocked_issues().unwrap();
    assert_eq!(blocked_issues.len(), 1);

    // Close blocker
    db.close_issue(blocker).unwrap();

    // Now should be ready
    let blocked_issues = db.list_blocked_issues().unwrap();
    assert!(blocked_issues.is_empty());

    let ready_issues = db.list_ready_issues().unwrap();
    assert!(ready_issues.iter().any(|i| i.id == blocked));
}

// ==================== Sessions Tests ====================

#[test]
fn test_start_and_get_session() {
    let (db, _dir) = setup_test_db();

    let id = db.start_session().unwrap();
    assert!(id > 0);

    let session = db.get_current_session().unwrap().unwrap();
    assert_eq!(session.id, id);
    assert!(session.ended_at.is_none());
    assert!(session.active_issue_id.is_none());
}

#[test]
fn test_end_session() {
    let (db, _dir) = setup_test_db();

    let id = db.start_session().unwrap();
    db.end_session(id, Some("Handoff notes")).unwrap();

    let current = db.get_current_session().unwrap();
    assert!(current.is_none());

    let last = db.get_last_session().unwrap().unwrap();
    assert_eq!(last.id, id);
    assert!(last.ended_at.is_some());
    assert_eq!(last.handoff_notes, Some("Handoff notes".to_string()));
}

#[test]
fn test_set_session_issue() {
    let (db, _dir) = setup_test_db();

    let issue_id = db.create_issue("Test issue", None, "medium").unwrap();
    let session_id = db.start_session().unwrap();

    db.set_session_issue(session_id, issue_id).unwrap();

    let session = db.get_current_session().unwrap().unwrap();
    assert_eq!(session.active_issue_id, Some(issue_id));
}

// ==================== Time Tracking Tests ====================

#[test]
fn test_start_and_stop_timer() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test issue", None, "medium").unwrap();

    let timer_id = db.start_timer(id).unwrap();
    assert!(timer_id > 0);

    let active = db.get_active_timer().unwrap();
    assert!(active.is_some());
    assert_eq!(active.unwrap().0, id);

    std::thread::sleep(std::time::Duration::from_millis(100));

    db.stop_timer(id).unwrap();

    let active = db.get_active_timer().unwrap();
    assert!(active.is_none());
}

#[test]
fn test_get_total_time() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test issue", None, "medium").unwrap();

    // No time tracked yet
    let total = db.get_total_time(id).unwrap();
    assert_eq!(total, 0);
}

// ==================== Search Tests ====================

#[test]
fn test_search_issues_by_title() {
    let (db, _dir) = setup_test_db();

    db.create_issue("Fix authentication bug", None, "high")
        .unwrap();
    db.create_issue("Add dark mode", None, "medium").unwrap();
    db.create_issue("Auth improvements", None, "low").unwrap();

    let results = db.search_issues("auth").unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn test_search_issues_by_description() {
    let (db, _dir) = setup_test_db();

    db.create_issue(
        "Feature A",
        Some("This relates to authentication"),
        "medium",
    )
    .unwrap();
    db.create_issue("Feature B", Some("Something else"), "medium")
        .unwrap();

    let results = db.search_issues("authentication").unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_search_issues_by_comment() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Some issue", None, "medium").unwrap();
    db.add_comment(id, "Found the root cause in authentication module", "note")
        .unwrap();

    let results = db.search_issues("authentication").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, id);
}

// ==================== Relations Tests ====================

#[test]
fn test_add_and_get_relations() {
    let (db, _dir) = setup_test_db();

    let id1 = db.create_issue("Issue 1", None, "medium").unwrap();
    let id2 = db.create_issue("Issue 2", None, "medium").unwrap();

    db.add_relation(id1, id2).unwrap();

    let related = db.get_related_issues(id1).unwrap();
    assert_eq!(related.len(), 1);
    assert_eq!(related[0].id, id2);

    // Bidirectional
    let related = db.get_related_issues(id2).unwrap();
    assert_eq!(related.len(), 1);
    assert_eq!(related[0].id, id1);
}

#[test]
fn test_relation_to_self_fails() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Issue", None, "medium").unwrap();

    let result = db.add_relation(id, id);
    assert!(result.is_err());
}

#[test]
fn test_remove_relation() {
    let (db, _dir) = setup_test_db();

    let id1 = db.create_issue("Issue 1", None, "medium").unwrap();
    let id2 = db.create_issue("Issue 2", None, "medium").unwrap();

    db.add_relation(id1, id2).unwrap();
    db.remove_typed_relation(id1, id2, "related").unwrap();

    let related = db.get_related_issues(id1).unwrap();
    assert!(related.is_empty());
}

// ==================== Milestones Tests ====================

#[test]
fn test_create_and_get_milestone() {
    let (db, _dir) = setup_test_db();

    let id = db.create_milestone("v1.0", Some("First release")).unwrap();
    assert!(id > 0);

    let milestone = db.get_milestone(id).unwrap().unwrap();
    assert_eq!(milestone.name, "v1.0");
    assert_eq!(milestone.description, Some("First release".to_string()));
    assert_eq!(milestone.status, "open");
}

#[test]
fn test_list_milestones() {
    let (db, _dir) = setup_test_db();

    db.create_milestone("v1.0", None).unwrap();
    db.create_milestone("v2.0", None).unwrap();

    let milestones = db.list_milestones(None).unwrap();
    assert_eq!(milestones.len(), 2);
}

#[test]
fn test_add_issue_to_milestone() {
    let (db, _dir) = setup_test_db();

    let milestone_id = db.create_milestone("v1.0", None).unwrap();
    let issue_id = db.create_issue("Feature", None, "medium").unwrap();

    db.add_issue_to_milestone(milestone_id, issue_id).unwrap();

    let issues = db.get_milestone_issues(milestone_id).unwrap();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].id, issue_id);

    let milestone = db.get_issue_milestone(issue_id).unwrap().unwrap();
    assert_eq!(milestone.id, milestone_id);
}

#[test]
fn test_close_milestone() {
    let (db, _dir) = setup_test_db();

    let id = db.create_milestone("v1.0", None).unwrap();
    db.close_milestone(id).unwrap();

    let milestone = db.get_milestone(id).unwrap().unwrap();
    assert_eq!(milestone.status, "closed");
    assert!(milestone.closed_at.is_some());
}

// ==================== Archive Tests ====================

#[test]
fn test_archive_closed_issue() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test", None, "medium").unwrap();
    db.close_issue(id).unwrap();

    let archived = db.archive_issue(id).unwrap();
    assert!(archived);

    let issue = db.get_issue(id).unwrap().unwrap();
    assert_eq!(issue.status, "archived");
}

#[test]
fn test_archive_open_issue_fails() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test", None, "medium").unwrap();

    let archived = db.archive_issue(id).unwrap();
    assert!(!archived);

    let issue = db.get_issue(id).unwrap().unwrap();
    assert_eq!(issue.status, "open");
}

#[test]
fn test_unarchive_issue() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test", None, "medium").unwrap();
    db.close_issue(id).unwrap();
    db.archive_issue(id).unwrap();

    let unarchived = db.unarchive_issue(id).unwrap();
    assert!(unarchived);

    let issue = db.get_issue(id).unwrap().unwrap();
    assert_eq!(issue.status, "closed");
}

#[test]
fn test_list_archived_issues() {
    let (db, _dir) = setup_test_db();

    let id1 = db.create_issue("Archived", None, "medium").unwrap();
    let _id2 = db.create_issue("Open", None, "medium").unwrap();

    db.close_issue(id1).unwrap();
    db.archive_issue(id1).unwrap();

    let archived = db.list_archived_issues().unwrap();
    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0].id, id1);
}

// ==================== Security Tests ====================

#[test]
fn test_sql_injection_in_title() {
    let (db, _dir) = setup_test_db();

    // Attempt SQL injection via title
    let malicious = "'; DROP TABLE issues; --";
    let id = db.create_issue(malicious, None, "medium").unwrap();

    // Should have created issue with literal string, not executed SQL
    let issue = db.get_issue(id).unwrap().unwrap();
    assert_eq!(issue.title, malicious);

    // Database should still be intact
    let issues = db.list_issues(None, &[], None).unwrap();
    assert!(!issues.is_empty());
}

#[test]
fn test_sql_injection_in_description() {
    let (db, _dir) = setup_test_db();

    let malicious = "test'); DELETE FROM issues; --";
    let id = db
        .create_issue("Normal title", Some(malicious), "medium")
        .unwrap();

    let issue = db.get_issue(id).unwrap().unwrap();
    assert_eq!(issue.description, Some(malicious.to_string()));
}

#[test]
fn test_sql_injection_in_label() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test", None, "medium").unwrap();
    let malicious = "bug'; DROP TABLE labels; --";

    db.add_label(id, malicious).unwrap();

    let labels = db.get_labels(id).unwrap();
    assert_eq!(labels.len(), 1);
    assert_eq!(labels[0], malicious);
}

#[test]
fn test_sql_injection_in_search() {
    let (db, _dir) = setup_test_db();

    db.create_issue("Normal issue", None, "medium").unwrap();

    // Attempt injection in search
    let malicious = "%'; DROP TABLE issues; --";
    let results = db.search_issues(malicious).unwrap();

    // Should return empty results, not crash
    assert!(results.is_empty());

    // Database should still be intact
    let issues = db.list_issues(None, &[], None).unwrap();
    assert!(!issues.is_empty());
}

#[test]
fn test_sql_injection_in_comment() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test", None, "medium").unwrap();
    let malicious = "comment'); DELETE FROM comments; --";

    db.add_comment(id, malicious, "note").unwrap();

    let comments = db.get_comments(id).unwrap();
    assert_eq!(comments.len(), 1);
    assert_eq!(comments[0].content, malicious);
}

#[test]
fn test_unicode_in_fields() {
    let (db, _dir) = setup_test_db();

    let title = "测试问题 🐛 αβγ";
    let description = "Description with émojis 🎉 and ñ";

    let id = db.create_issue(title, Some(description), "medium").unwrap();

    let issue = db.get_issue(id).unwrap().unwrap();
    assert_eq!(issue.title, title);
    assert_eq!(issue.description, Some(description.to_string()));
}

#[test]
fn test_very_long_strings() {
    let (db, _dir) = setup_test_db();

    // Within limits: should succeed
    let long_title = "a".repeat(MAX_TITLE_LEN);
    let long_desc = "b".repeat(MAX_DESCRIPTION_LEN);

    let id = db
        .create_issue(&long_title, Some(&long_desc), "medium")
        .unwrap();

    let issue = db.get_issue(id).unwrap().unwrap();
    assert_eq!(issue.title.len(), MAX_TITLE_LEN);
    assert_eq!(issue.description.unwrap().len(), MAX_DESCRIPTION_LEN);

    // Exceeding limits: should fail
    let too_long_title = "a".repeat(MAX_TITLE_LEN + 1);
    assert!(db.create_issue(&too_long_title, None, "medium").is_err());

    let too_long_desc = "b".repeat(MAX_DESCRIPTION_LEN + 1);
    assert!(db
        .create_issue("ok", Some(&too_long_desc), "medium")
        .is_err());
}

#[test]
fn test_null_bytes_in_strings() {
    let (db, _dir) = setup_test_db();

    let title = "test\0null\0bytes";
    let id = db.create_issue(title, None, "medium").unwrap();

    let issue = db.get_issue(id).unwrap().unwrap();
    assert_eq!(issue.title, title);
}

// ==================== Cascade Delete Tests ====================

#[test]
fn test_delete_issue_cascades_labels() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test", None, "medium").unwrap();
    db.add_label(id, "bug").unwrap();
    db.add_label(id, "urgent").unwrap();

    db.delete_issue(id).unwrap();

    // Labels should be gone (via CASCADE)
    let labels = db.get_labels(id).unwrap();
    assert!(labels.is_empty());
}

#[test]
fn test_delete_issue_cascades_comments() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("Test", None, "medium").unwrap();
    db.add_comment(id, "Comment 1", "note").unwrap();
    db.add_comment(id, "Comment 2", "note").unwrap();

    db.delete_issue(id).unwrap();

    let comments = db.get_comments(id).unwrap();
    assert!(comments.is_empty());
}

#[test]
fn test_delete_parent_cascades_subissues() {
    let (db, _dir) = setup_test_db();

    let parent_id = db.create_issue("Parent", None, "high").unwrap();
    let child_id = db
        .create_subissue(parent_id, "Child", None, "medium")
        .unwrap();

    db.delete_issue(parent_id).unwrap();

    // Child should be deleted too
    assert!(db.get_issue(child_id).unwrap().is_none());
}

// ==================== Edge Cases ====================

#[test]
fn test_empty_title() {
    let (db, _dir) = setup_test_db();

    let id = db.create_issue("", None, "medium").unwrap();
    let issue = db.get_issue(id).unwrap().unwrap();
    assert_eq!(issue.title, "");
}

#[test]
fn test_update_parent() {
    let (db, _dir) = setup_test_db();

    let parent1 = db.create_issue("Parent 1", None, "high").unwrap();
    let parent2 = db.create_issue("Parent 2", None, "high").unwrap();
    let child = db.create_issue("Child", None, "medium").unwrap();

    db.update_parent(child, Some(parent1)).unwrap();
    let issue = db.get_issue(child).unwrap().unwrap();
    assert_eq!(issue.parent_id, Some(parent1));

    db.update_parent(child, Some(parent2)).unwrap();
    let issue = db.get_issue(child).unwrap().unwrap();
    assert_eq!(issue.parent_id, Some(parent2));

    db.update_parent(child, None).unwrap();
    let issue = db.get_issue(child).unwrap().unwrap();
    assert_eq!(issue.parent_id, None);
}

// ==================== Database Corruption Recovery ====================

#[test]
fn test_corrupted_db_file_empty() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("issues.db");

    // Create an empty file (corrupted)
    std::fs::write(&db_path, b"").unwrap();

    // SQLite treats empty files as new databases, so this should succeed
    // and the database should be usable afterward
    let result = Database::open(&db_path);
    assert!(
        result.is_ok(),
        "Empty file should be treated as new DB: {:?}",
        result.err()
    );
    let db = result.unwrap();
    let id = db
        .create_issue("Test after recovery", None, "medium")
        .unwrap();
    assert!(id > 0);
}

#[test]
fn test_corrupted_db_file_garbage() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("issues.db");

    // Write garbage data
    std::fs::write(&db_path, b"not a sqlite database at all!").unwrap();

    // Should fail gracefully with an error, not panic
    let result = Database::open(&db_path);
    assert!(result.is_err());
}

#[test]
fn test_corrupted_db_file_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("issues.db");

    // Create valid DB first
    {
        let db = Database::open(&db_path).unwrap();
        db.create_issue("Test", None, "medium").unwrap();
    }

    // Truncate it (simulate crash during write)
    let content = std::fs::read(&db_path).unwrap();
    std::fs::write(&db_path, &content[..content.len() / 2]).unwrap();

    // Truncated DB should fail to open -- SQLite detects corruption
    let result = Database::open(&db_path);
    match result {
        Err(e) => {
            let err_msg = format!("{}", e);
            assert!(
                err_msg.contains("not a database")
                    || err_msg.contains("malformed")
                    || err_msg.contains("corrupt")
                    || err_msg.contains("disk image"),
                "Error should indicate corruption, got: {}",
                err_msg
            );
        }
        Ok(db) => {
            // If SQLite somehow recovers, verify the original data is gone
            let issues = db.list_issues(Some("all"), &[], None).unwrap();
            assert!(
                issues.is_empty(),
                "Truncated DB should not retain original data"
            );
        }
    }
}

#[test]
fn test_db_readonly_location() {
    // This test only works on Unix-like systems
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("issues.db");

        // Create the file first
        std::fs::write(&db_path, b"").unwrap();

        // Make it read-only
        let mut perms = std::fs::metadata(&db_path).unwrap().permissions();
        perms.set_mode(0o444);
        std::fs::set_permissions(&db_path, perms).unwrap();

        // Should fail gracefully
        let result = Database::open(&db_path);
        assert!(result.is_err());
    }
}
