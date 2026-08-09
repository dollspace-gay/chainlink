use anyhow::{bail, Result};

use crate::db::Database;
use crate::utils::format_issue_id;

pub fn archive(db: &Database, id: i64) -> Result<()> {
    let issue = match db.get_issue(id)? {
        Some(i) => i,
        None => bail!("Issue {} not found", format_issue_id(id)),
    };

    if issue.status != "closed" {
        bail!(
            "Can only archive closed issues. Issue {} is '{}'",
            format_issue_id(id),
            issue.status
        );
    }

    if db.archive_issue(id)? {
        println!("Archived issue {}", format_issue_id(id));
    } else {
        println!("Issue {} could not be archived", format_issue_id(id));
    }

    Ok(())
}

pub fn unarchive(db: &Database, id: i64) -> Result<()> {
    if db.unarchive_issue(id)? {
        println!("Unarchived issue {} (now closed)", format_issue_id(id));
    } else {
        bail!("Issue {} not found or not archived", format_issue_id(id));
    }

    Ok(())
}

pub fn list(db: &Database) -> Result<()> {
    let issues = db.list_archived_issues()?;

    if issues.is_empty() {
        println!("No archived issues.");
        return Ok(());
    }

    println!("Archived issues:\n");
    for issue in issues {
        let parent_str = issue
            .parent_id
            .map(|p| format!(" (sub of {})", format_issue_id(p)))
            .unwrap_or_default();
        println!(
            "{:<5} {:8} {}{}",
            format_issue_id(issue.id),
            issue.priority,
            issue.title,
            parent_str
        );
    }

    Ok(())
}

pub fn archive_older(db: &Database, days: i64) -> Result<()> {
    let count = db.archive_older_than(days)?;
    if count > 0 {
        println!(
            "Archived {} issue(s) closed more than {} days ago",
            count, days
        );
    } else {
        println!(
            "No issues to archive (none closed more than {} days ago)",
            days
        );
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

    #[test]
    fn test_archive_closed_issue() {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test issue", None, "medium").unwrap();
        db.close_issue(id).unwrap();

        archive(&db, id).unwrap();
        let archived = db.list_archived_issues().unwrap();
        assert!(
            archived.iter().any(|i| i.id == id),
            "Issue should appear in archived list"
        );
    }

    #[test]
    fn test_archive_open_issue_fails() {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test issue", None, "medium").unwrap();

        let result = archive(&db, id);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("only archive closed"));
    }

    #[test]
    fn test_archive_nonexistent_fails() {
        let (db, _dir) = setup_test_db();

        let result = archive(&db, 99999);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_unarchive_issue() {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test issue", None, "medium").unwrap();
        db.close_issue(id).unwrap();
        archive(&db, id).unwrap();

        unarchive(&db, id).unwrap();
        let archived = db.list_archived_issues().unwrap();
        assert!(
            !archived.iter().any(|i| i.id == id),
            "Issue should no longer be archived"
        );
        let closed = db.list_issues(Some("closed"), &[], None).unwrap();
        assert!(
            closed.iter().any(|i| i.id == id),
            "Issue should be back in closed list"
        );
    }

    #[test]
    fn test_unarchive_not_archived() {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test issue", None, "medium").unwrap();

        let result = unarchive(&db, id);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not found or not archived"));
    }

    #[test]
    fn test_list_empty() {
        let (db, _dir) = setup_test_db();

        list(&db).unwrap();
        let archived = db.list_archived_issues().unwrap();
        assert!(archived.is_empty());
    }

    #[test]
    fn test_list_with_archived() {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test issue", None, "medium").unwrap();
        db.close_issue(id).unwrap();
        archive(&db, id).unwrap();

        list(&db).unwrap();
        let archived = db.list_archived_issues().unwrap();
        assert_eq!(archived.len(), 1);
        assert_eq!(archived[0].id, id);
    }

    #[test]
    fn test_archive_older_none() {
        let (db, _dir) = setup_test_db();

        archive_older(&db, 30).unwrap();
        let archived = db.list_archived_issues().unwrap();
        assert!(
            archived.is_empty(),
            "No issues should be archived with empty DB"
        );
    }

    #[test]
    fn test_archive_unarchive_roundtrip() {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test issue", None, "medium").unwrap();
        db.close_issue(id).unwrap();

        archive(&db, id).unwrap();
        let archived = db.list_archived_issues().unwrap();
        assert!(archived.iter().any(|i| i.id == id));

        unarchive(&db, id).unwrap();
        let archived = db.list_archived_issues().unwrap();
        assert!(!archived.iter().any(|i| i.id == id));
    }

    #[test]
    fn test_archived_issue_not_in_open_or_closed_list() {
        let (db, _dir) = setup_test_db();
        let id = db.create_issue("Test issue", None, "medium").unwrap();
        db.close_issue(id).unwrap();
        archive(&db, id).unwrap();

        let open_issues = db.list_issues(Some("open"), &[], None).unwrap();
        let closed_issues = db.list_issues(Some("closed"), &[], None).unwrap();
        assert!(!open_issues.iter().any(|i| i.id == id));
        assert!(!closed_issues.iter().any(|i| i.id == id));
    }

    proptest! {
        #[test]
        fn prop_archive_requires_closed(title in "[a-zA-Z0-9 ]{1,30}") {
            let (db, _dir) = setup_test_db();
            let id = db.create_issue(&title, None, "medium").unwrap();

            let result = archive(&db, id);
            prop_assert!(result.is_err());
        }

        #[test]
        fn prop_archive_closed_succeeds(title in "[a-zA-Z0-9 ]{1,30}") {
            let (db, _dir) = setup_test_db();
            let id = db.create_issue(&title, None, "medium").unwrap();
            db.close_issue(id).unwrap();

            archive(&db, id).unwrap();
            let archived = db.list_archived_issues().unwrap();
            prop_assert!(archived.iter().any(|i| i.id == id));
        }
    }
}
