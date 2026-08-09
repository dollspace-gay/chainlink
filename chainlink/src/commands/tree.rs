use anyhow::Result;

use crate::db::Database;
use crate::models::Issue;

fn status_icon(status: &str) -> &'static str {
    match status {
        "open" => " ",
        "closed" => "x",
        _ => "?",
    }
}

fn print_issue(issue: &Issue, indent: usize) {
    let prefix = "  ".repeat(indent);
    let icon = status_icon(&issue.status);
    println!(
        "{}[{}] #{} {} - {}",
        prefix, icon, issue.id, issue.priority, issue.title
    );
}

fn print_tree_recursive(
    db: &Database,
    parent_id: i64,
    indent: usize,
    status_filter: Option<&str>,
) -> Result<()> {
    let subissues = db.get_subissues(parent_id)?;
    for sub in subissues {
        let dominated_by_filter = match status_filter {
            Some("all") | None => false,
            Some(filter) => sub.status != filter,
        };
        if dominated_by_filter {
            continue;
        }
        print_issue(&sub, indent);
        print_tree_recursive(db, sub.id, indent + 1, status_filter)?;
    }
    Ok(())
}

pub fn run(db: &Database, status_filter: Option<&str>) -> Result<()> {
    // Get all top-level issues (no parent)
    let all_issues = db.list_issues(status_filter, &[], None)?;
    let top_level: Vec<_> = all_issues
        .into_iter()
        .filter(|i| i.parent_id.is_none())
        .collect();

    if top_level.is_empty() {
        println!("No issues found.");
        return Ok(());
    }

    for issue in top_level {
        print_issue(&issue, 0);
        print_tree_recursive(db, issue.id, 1, status_filter)?;
    }

    // Legend
    println!();
    println!("Legend: [ ] open, [x] closed");

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
    fn test_status_icon_open() {
        assert_eq!(status_icon("open"), " ");
    }

    #[test]
    fn test_status_icon_closed() {
        assert_eq!(status_icon("closed"), "x");
    }

    #[test]
    fn test_status_icon_unknown() {
        assert_eq!(status_icon("archived"), "?");
    }

    #[test]
    fn test_run_empty() {
        let (db, _dir) = setup_test_db();
        run(&db, None).unwrap();
        let issues = db.list_issues(None, &[], None).unwrap();
        assert!(issues.is_empty());
    }

    #[test]
    fn test_run_single_issue() {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test issue", None, "medium").unwrap();
        run(&db, None).unwrap();
        let issues = db.list_issues(None, &[], None).unwrap();
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].id, id);
    }

    #[test]
    fn test_run_with_hierarchy() {
        let (db, _dir) = setup_test_db();
        let parent = db.create_issue("Parent", None, "high").unwrap();
        let c1 = db
            .create_subissue(parent, "Child 1", None, "medium")
            .unwrap();
        let c2 = db.create_subissue(parent, "Child 2", None, "low").unwrap();
        run(&db, None).unwrap();
        let subs = db.get_subissues(parent).unwrap();
        assert_eq!(subs.len(), 2);
        assert!(subs.iter().any(|s| s.id == c1));
        assert!(subs.iter().any(|s| s.id == c2));
    }

    #[test]
    fn test_run_nested_hierarchy() {
        let (db, _dir) = setup_test_db();
        let grandparent = db.create_issue("Grandparent", None, "high").unwrap();
        let parent = db
            .create_subissue(grandparent, "Parent", None, "medium")
            .unwrap();
        let child = db.create_subissue(parent, "Child", None, "low").unwrap();
        run(&db, None).unwrap();
        let child_issue = db.get_issue(child).unwrap().unwrap();
        assert_eq!(child_issue.parent_id, Some(parent));
        let parent_issue = db.get_issue(parent).unwrap().unwrap();
        assert_eq!(parent_issue.parent_id, Some(grandparent));
    }

    #[test]
    fn test_run_with_status_filter() {
        let (db, _dir) = setup_test_db();
        let closed_id = db.create_issue("Closed issue", None, "medium").unwrap();
        let open_id = db.create_issue("Open issue", None, "medium").unwrap();
        db.close_issue(closed_id).unwrap();
        run(&db, Some("open")).unwrap();
        let open_issues = db.list_issues(Some("open"), &[], None).unwrap();
        assert_eq!(open_issues.len(), 1);
        assert_eq!(open_issues[0].id, open_id);
    }

    #[test]
    fn test_run_closed_filter() {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Issue", None, "medium").unwrap();
        db.close_issue(id).unwrap();
        run(&db, Some("closed")).unwrap();
        let closed = db.list_issues(Some("closed"), &[], None).unwrap();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].id, id);
    }

    #[test]
    fn test_run_all_filter() {
        let (db, _dir) = setup_test_db();
        db.create_issue("Open issue", None, "medium").unwrap();
        let id = db.create_issue("Closed issue", None, "medium").unwrap();
        db.close_issue(id).unwrap();
        run(&db, Some("all")).unwrap();
        let all = db.list_issues(Some("all"), &[], None).unwrap();
        assert_eq!(all.len(), 2);
    }

    proptest! {
        #[test]
        fn prop_run_never_panics(count in 0usize..5) {
            let (db, _dir) = setup_test_db();
            for i in 0..count {
                db.create_issue(&format!("Issue {}", i), None, "medium").unwrap();
            }
            let result = run(&db, None);
            prop_assert!(result.is_ok());
        }

        #[test]
        fn prop_hierarchy_never_panics(depth in 1usize..4) {
            let (db, _dir) = setup_test_db();
            let mut parent_id = db.create_issue("Root", None, "high").unwrap();
            for i in 0..depth {
                parent_id = db.create_subissue(parent_id, &format!("Child {}", i), None, "medium").unwrap();
            }
            let result = run(&db, None);
            prop_assert!(result.is_ok());
        }
    }
}
