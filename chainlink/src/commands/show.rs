use anyhow::{bail, Result};
use serde::Serialize;
use serde_json;

use crate::db::Database;
use crate::utils::format_issue_id;

#[derive(Serialize)]
struct IssueDetail {
    #[serde(flatten)]
    issue: crate::models::Issue,
    labels: Vec<String>,
    milestone: Option<crate::models::Milestone>,
    comments: Vec<crate::models::Comment>,
    blocked_by: Vec<i64>,
    blocked_by_open: Vec<i64>,
    blocking: Vec<i64>,
    subissues: Vec<crate::models::Issue>,
    related: Vec<crate::models::Issue>,
    relations: Vec<crate::models::Relation>,
}

pub fn run_json(db: &Database, id: i64) -> Result<()> {
    let issue = match db.get_issue(id)? {
        Some(i) => i,
        None => bail!("Issue {} not found", format_issue_id(id)),
    };

    let detail = IssueDetail {
        issue,
        labels: db.get_labels(id)?,
        milestone: db.get_issue_milestone(id)?,
        comments: db.get_comments(id)?,
        blocked_by: db.get_blockers(id)?,
        blocked_by_open: db.get_open_blockers(id)?,
        blocking: db.get_blocking(id)?,
        subissues: db.get_subissues(id)?,
        related: db.get_related_issues(id)?,
        relations: db.get_typed_relations(id)?,
    };

    println!("{}", serde_json::to_string_pretty(&detail)?);
    Ok(())
}

pub fn run(db: &Database, id: i64) -> Result<()> {
    let issue = match db.get_issue(id)? {
        Some(i) => i,
        None => bail!("Issue {} not found", format_issue_id(id)),
    };

    println!("Issue {}: {}", format_issue_id(issue.id), issue.title);
    println!("Status: {}", issue.status);
    println!("Priority: {}", issue.priority);
    if let Some(parent_id) = issue.parent_id {
        println!("Parent: {}", format_issue_id(parent_id));
    }
    println!("Created: {}", issue.created_at.format("%Y-%m-%d %H:%M:%S"));
    println!("Updated: {}", issue.updated_at.format("%Y-%m-%d %H:%M:%S"));

    if let Some(closed) = issue.closed_at {
        println!("Closed: {}", closed.format("%Y-%m-%d %H:%M:%S"));
    }

    // Labels
    let labels = db.get_labels(id)?;
    if !labels.is_empty() {
        println!("Labels: {}", labels.join(", "));
    }

    // Milestone
    if let Some(milestone) = db.get_issue_milestone(id)? {
        println!(
            "Milestone: {} {}",
            format_issue_id(milestone.id),
            milestone.name
        );
    }

    // Description
    if let Some(desc) = &issue.description {
        if !desc.is_empty() {
            println!("\nDescription:");
            for line in desc.lines() {
                println!("  {}", line);
            }
        }
    }

    // Comments
    let comments = db.get_comments(id)?;
    if !comments.is_empty() {
        println!("\nComments:");
        for comment in comments {
            println!(
                "  [{}] {}",
                comment.created_at.format("%Y-%m-%d %H:%M"),
                comment.content
            );
        }
    }

    // Dependencies
    let blockers = db.get_blockers(id)?;
    let open_blockers = db.get_open_blockers(id)?;
    let blocking = db.get_blocking(id)?;

    println!();
    if blockers.is_empty() {
        println!("Blocked by: (none)");
    } else {
        let blocker_strs: Vec<String> = blockers.iter().map(|b| format_issue_id(*b)).collect();
        println!("Blocked by: {}", blocker_strs.join(", "));
    }

    if !blockers.is_empty() {
        if open_blockers.is_empty() {
            println!("Blocked by (open): (none)");
        } else {
            let open_strs: Vec<String> =
                open_blockers.iter().map(|b| format_issue_id(*b)).collect();
            println!("Blocked by (open): {}", open_strs.join(", "));
        }
    }

    if blocking.is_empty() {
        println!("Blocking: (none)");
    } else {
        let blocking_strs: Vec<String> = blocking.iter().map(|b| format_issue_id(*b)).collect();
        println!("Blocking: {}", blocking_strs.join(", "));
    }

    // Subissues
    let subissues = db.get_subissues(id)?;
    if !subissues.is_empty() {
        println!("\nSubissues:");
        for sub in subissues {
            println!(
                "  {} [{}] {} - {}",
                format_issue_id(sub.id),
                sub.status,
                sub.priority,
                sub.title
            );
        }
    }

    // Related issues (grouped by type)
    let relations = db.get_typed_relations(id)?;
    if !relations.is_empty() {
        println!("\nRelations:");

        let mut by_type: std::collections::BTreeMap<String, Vec<i64>> =
            std::collections::BTreeMap::new();
        for rel in &relations {
            let other_id = if rel.issue_id_1 == id {
                rel.issue_id_2
            } else {
                rel.issue_id_1
            };
            by_type
                .entry(rel.relation_type.clone())
                .or_default()
                .push(other_id);
        }

        for (rel_type, ids) in &by_type {
            println!("  [{}]:", rel_type);
            for &other_id in ids {
                if let Some(other) = db.get_issue(other_id)? {
                    let status_marker = if other.status == "closed" { "✓" } else { " " };
                    println!(
                        "    {} [{}] {} - {}",
                        format_issue_id(other.id),
                        status_marker,
                        other.priority,
                        other.title
                    );
                }
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
    fn test_show_existing_issue() {
        let (db, _dir) = setup_test_db();
        let issue_id = db.create_issue("Test issue", None, "medium").unwrap();

        run(&db, issue_id).unwrap();
        let issue = db.get_issue(issue_id).unwrap().unwrap();
        assert_eq!(issue.title, "Test issue");
        assert_eq!(issue.priority, "medium");
        assert_eq!(issue.status, "open");
    }

    #[test]
    fn test_show_nonexistent_issue() {
        let (db, _dir) = setup_test_db();

        let result = run(&db, 99999);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_show_issue_with_description() {
        let (db, _dir) = setup_test_db();
        let issue_id = db
            .create_issue("Test issue", Some("A detailed description"), "high")
            .unwrap();

        run(&db, issue_id).unwrap();
        let issue = db.get_issue(issue_id).unwrap().unwrap();
        assert_eq!(
            issue.description,
            Some("A detailed description".to_string())
        );
    }

    #[test]
    fn test_show_issue_with_labels() {
        let (db, _dir) = setup_test_db();
        let issue_id = db.create_issue("Test issue", None, "medium").unwrap();
        db.add_label(issue_id, "bug").unwrap();
        db.add_label(issue_id, "urgent").unwrap();

        run(&db, issue_id).unwrap();
        let labels = db.get_labels(issue_id).unwrap();
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&"bug".to_string()));
        assert!(labels.contains(&"urgent".to_string()));
    }

    #[test]
    fn test_show_issue_with_comments() {
        let (db, _dir) = setup_test_db();
        let issue_id = db.create_issue("Test issue", None, "medium").unwrap();
        db.add_comment(issue_id, "First comment", "note").unwrap();
        db.add_comment(issue_id, "Second comment", "note").unwrap();

        run(&db, issue_id).unwrap();
        let comments = db.get_comments(issue_id).unwrap();
        assert_eq!(comments.len(), 2);
        assert_eq!(comments[0].content, "First comment");
        assert_eq!(comments[1].content, "Second comment");
    }

    #[test]
    fn test_show_issue_with_blockers() {
        let (db, _dir) = setup_test_db();
        let blocker_id = db.create_issue("Blocker", None, "high").unwrap();
        let issue_id = db.create_issue("Blocked issue", None, "medium").unwrap();
        db.add_dependency(issue_id, blocker_id).unwrap();

        run(&db, issue_id).unwrap();
        let blockers = db.get_blockers(issue_id).unwrap();
        assert_eq!(blockers.len(), 1);
        assert!(blockers.contains(&blocker_id));
    }

    #[test]
    fn test_show_issue_with_subissues() {
        let (db, _dir) = setup_test_db();
        let parent_id = db.create_issue("Parent", None, "high").unwrap();
        let c1 = db
            .create_subissue(parent_id, "Child 1", None, "medium")
            .unwrap();
        let c2 = db
            .create_subissue(parent_id, "Child 2", None, "low")
            .unwrap();

        run(&db, parent_id).unwrap();
        let subs = db.get_subissues(parent_id).unwrap();
        assert_eq!(subs.len(), 2);
        assert!(subs.iter().any(|s| s.id == c1 && s.title == "Child 1"));
        assert!(subs.iter().any(|s| s.id == c2 && s.title == "Child 2"));
    }

    #[test]
    fn test_show_subissue_shows_parent() {
        let (db, _dir) = setup_test_db();
        let parent_id = db.create_issue("Parent", None, "high").unwrap();
        let child_id = db
            .create_subissue(parent_id, "Child", None, "medium")
            .unwrap();

        run(&db, child_id).unwrap();
        let child = db.get_issue(child_id).unwrap().unwrap();
        assert_eq!(child.parent_id, Some(parent_id));
    }

    #[test]
    fn test_show_issue_with_related() {
        let (db, _dir) = setup_test_db();
        let issue1 = db.create_issue("Issue 1", None, "medium").unwrap();
        let issue2 = db.create_issue("Issue 2", None, "medium").unwrap();
        db.add_relation(issue1, issue2).unwrap();

        run(&db, issue1).unwrap();
        let related = db.get_related_issues(issue1).unwrap();
        assert_eq!(related.len(), 1);
        assert_eq!(related[0].id, issue2);
    }

    #[test]
    fn test_show_closed_issue() {
        let (db, _dir) = setup_test_db();
        let issue_id = db.create_issue("Test issue", None, "medium").unwrap();
        db.close_issue(issue_id).unwrap();

        run(&db, issue_id).unwrap();
        let issue = db.get_issue(issue_id).unwrap().unwrap();
        assert_eq!(issue.status, "closed");
        assert!(issue.closed_at.is_some());
    }

    #[test]
    fn test_show_issue_with_milestone() {
        let (db, _dir) = setup_test_db();
        let issue_id = db.create_issue("Test issue", None, "medium").unwrap();
        let milestone_id = db.create_milestone("v1.0", None).unwrap();
        db.add_issue_to_milestone(milestone_id, issue_id).unwrap();

        run(&db, issue_id).unwrap();
        let milestone = db.get_issue_milestone(issue_id).unwrap();
        assert!(milestone.is_some());
        assert_eq!(milestone.unwrap().name, "v1.0");
    }

    #[test]
    fn test_show_issue_unicode_content() {
        let (db, _dir) = setup_test_db();
        let issue_id = db
            .create_issue("测试问题 🐛", Some("描述 αβγ"), "medium")
            .unwrap();
        db.add_comment(issue_id, "评论 🎉", "note").unwrap();
        db.add_label(issue_id, "バグ").unwrap();

        run(&db, issue_id).unwrap();
        let issue = db.get_issue(issue_id).unwrap().unwrap();
        assert_eq!(issue.title, "测试问题 🐛");
        assert_eq!(issue.description, Some("描述 αβγ".to_string()));
        let labels = db.get_labels(issue_id).unwrap();
        assert!(labels.contains(&"バグ".to_string()));
    }

    #[test]
    fn test_show_issue_multiline_description() {
        let (db, _dir) = setup_test_db();
        let desc = "Line 1\nLine 2\n\nLine 4 after blank";
        let issue_id = db.create_issue("Test", Some(desc), "medium").unwrap();

        run(&db, issue_id).unwrap();
        let issue = db.get_issue(issue_id).unwrap().unwrap();
        assert_eq!(issue.description, Some(desc.to_string()));
    }

    #[test]
    fn test_show_issue_empty_description() {
        let (db, _dir) = setup_test_db();
        let issue_id = db.create_issue("Test", Some(""), "medium").unwrap();

        run(&db, issue_id).unwrap();
        let issue = db.get_issue(issue_id).unwrap().unwrap();
        assert_eq!(issue.description, Some("".to_string()));
    }

    #[test]
    fn test_blocked_by_open_all_open() {
        let (db, _dir) = setup_test_db();
        let blocker1 = db.create_issue("Blocker 1", None, "high").unwrap();
        let blocker2 = db.create_issue("Blocker 2", None, "high").unwrap();
        let issue_id = db.create_issue("Blocked", None, "medium").unwrap();
        db.add_dependency(issue_id, blocker1).unwrap();
        db.add_dependency(issue_id, blocker2).unwrap();

        let open = db.get_open_blockers(issue_id).unwrap();
        assert_eq!(open.len(), 2);
        assert!(open.contains(&blocker1));
        assert!(open.contains(&blocker2));
    }

    #[test]
    fn test_blocked_by_open_filters_closed() {
        let (db, _dir) = setup_test_db();
        let open_blocker = db.create_issue("Open blocker", None, "high").unwrap();
        let closed_blocker = db.create_issue("Closed blocker", None, "high").unwrap();
        let issue_id = db.create_issue("Blocked", None, "medium").unwrap();
        db.add_dependency(issue_id, open_blocker).unwrap();
        db.add_dependency(issue_id, closed_blocker).unwrap();
        db.close_issue(closed_blocker).unwrap();

        let all_blockers = db.get_blockers(issue_id).unwrap();
        assert_eq!(
            all_blockers.len(),
            2,
            "get_blockers should still return all"
        );

        let open = db.get_open_blockers(issue_id).unwrap();
        assert_eq!(open.len(), 1);
        assert!(open.contains(&open_blocker));
        assert!(!open.contains(&closed_blocker));
    }

    #[test]
    fn test_blocked_by_open_all_closed() {
        let (db, _dir) = setup_test_db();
        let blocker1 = db.create_issue("Blocker 1", None, "high").unwrap();
        let blocker2 = db.create_issue("Blocker 2", None, "high").unwrap();
        let issue_id = db.create_issue("Blocked", None, "medium").unwrap();
        db.add_dependency(issue_id, blocker1).unwrap();
        db.add_dependency(issue_id, blocker2).unwrap();
        db.close_issue(blocker1).unwrap();
        db.close_issue(blocker2).unwrap();

        let all_blockers = db.get_blockers(issue_id).unwrap();
        assert_eq!(all_blockers.len(), 2);

        let open = db.get_open_blockers(issue_id).unwrap();
        assert!(open.is_empty());
    }

    #[test]
    fn test_blocked_by_open_json_field() {
        let (db, _dir) = setup_test_db();
        let open_blocker = db.create_issue("Open blocker", None, "high").unwrap();
        let closed_blocker = db.create_issue("Closed blocker", None, "high").unwrap();
        let issue_id = db.create_issue("Blocked", None, "medium").unwrap();
        db.add_dependency(issue_id, open_blocker).unwrap();
        db.add_dependency(issue_id, closed_blocker).unwrap();
        db.close_issue(closed_blocker).unwrap();

        let detail = IssueDetail {
            issue: db.get_issue(issue_id).unwrap().unwrap(),
            labels: db.get_labels(issue_id).unwrap(),
            milestone: None,
            comments: db.get_comments(issue_id).unwrap(),
            blocked_by: db.get_blockers(issue_id).unwrap(),
            blocked_by_open: db.get_open_blockers(issue_id).unwrap(),
            blocking: db.get_blocking(issue_id).unwrap(),
            subissues: db.get_subissues(issue_id).unwrap(),
            related: db.get_related_issues(issue_id).unwrap(),
            relations: db.get_typed_relations(issue_id).unwrap(),
        };

        let json = serde_json::to_value(&detail).unwrap();
        let blocked = json["blocked_by"].as_array().unwrap();
        assert_eq!(blocked.len(), 2, "blocked_by should include all blockers");

        let open = json["blocked_by_open"].as_array().unwrap();
        assert_eq!(open.len(), 1, "blocked_by_open should filter closed");
        assert!(open.iter().any(|v| v.as_i64() == Some(open_blocker)));
    }

    // ==================== Property-Based Tests ====================

    proptest! {
        #[test]
        fn prop_show_never_panics(title in "[a-zA-Z0-9 ]{1,50}") {
            let (db, _dir) = setup_test_db();
            let issue_id = db.create_issue(&title, None, "medium").unwrap();
            let result = run(&db, issue_id);
            prop_assert!(result.is_ok());
        }

        #[test]
        fn prop_show_nonexistent_always_fails(issue_id in 1000i64..10000) {
            let (db, _dir) = setup_test_db();
            let result = run(&db, issue_id);
            prop_assert!(result.is_err());
        }

        #[test]
        fn prop_show_with_description_never_panics(
            title in "[a-zA-Z0-9 ]{1,30}",
            desc in "[a-zA-Z0-9 \n]{0,200}"
        ) {
            let (db, _dir) = setup_test_db();
            let issue_id = db.create_issue(&title, Some(&desc), "medium").unwrap();
            let result = run(&db, issue_id);
            prop_assert!(result.is_ok());
        }

        #[test]
        fn prop_show_unicode_never_panics(
            title in "[\\p{L}\\p{N} ]{1,30}"
        ) {
            let (db, _dir) = setup_test_db();
            let issue_id = db.create_issue(&title, None, "medium").unwrap();
            let result = run(&db, issue_id);
            prop_assert!(result.is_ok());
        }
    }
}
