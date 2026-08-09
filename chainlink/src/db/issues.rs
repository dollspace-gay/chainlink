use anyhow::Result;
use chrono::Utc;
use rusqlite::params;

use super::{issue_from_row, validate_priority, validate_status, Database};
use super::{MAX_DESCRIPTION_LEN, MAX_TITLE_LEN};
use crate::models::Issue;

impl Database {
    pub fn create_issue(
        &self,
        title: &str,
        description: Option<&str>,
        priority: &str,
    ) -> Result<i64> {
        self.create_issue_with_parent(title, description, priority, None)
    }

    pub fn create_subissue(
        &self,
        parent_id: i64,
        title: &str,
        description: Option<&str>,
        priority: &str,
    ) -> Result<i64> {
        self.create_issue_with_parent(title, description, priority, Some(parent_id))
    }

    fn create_issue_with_parent(
        &self,
        title: &str,
        description: Option<&str>,
        priority: &str,
        parent_id: Option<i64>,
    ) -> Result<i64> {
        validate_priority(priority)?;
        if title.len() > MAX_TITLE_LEN {
            anyhow::bail!(
                "Title exceeds maximum length of {} characters",
                MAX_TITLE_LEN
            );
        }
        if let Some(d) = description {
            if d.len() > MAX_DESCRIPTION_LEN {
                anyhow::bail!(
                    "Description exceeds maximum length of {} bytes",
                    MAX_DESCRIPTION_LEN
                );
            }
        }
        let now = Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO issues (title, description, priority, parent_id, status, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, 'open', ?5, ?5)",
            params![title, description, priority, parent_id, now],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn get_subissues(&self, parent_id: i64) -> Result<Vec<Issue>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, title, description, status, priority, parent_id, created_at, updated_at, closed_at FROM issues WHERE parent_id = ?1 ORDER BY id",
        )?;

        let issues = stmt
            .query_map([parent_id], issue_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(issues)
    }

    pub fn get_issue(&self, id: i64) -> Result<Option<Issue>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, title, description, status, priority, parent_id, created_at, updated_at, closed_at FROM issues WHERE id = ?1",
        )?;

        let issue = stmt.query_row([id], issue_from_row).ok();

        Ok(issue)
    }

    /// Get an issue by ID, returning an error if not found.
    pub fn require_issue(&self, id: i64) -> Result<Issue> {
        self.get_issue(id)?
            .ok_or_else(|| anyhow::anyhow!("Issue {} not found", crate::utils::format_issue_id(id)))
    }

    pub fn list_issues(
        &self,
        status_filter: Option<&str>,
        label_filters: &[String],
        priority_filter: Option<&str>,
    ) -> Result<Vec<Issue>> {
        let mut sql = String::from(
            "SELECT DISTINCT i.id, i.title, i.description, i.status, i.priority, i.parent_id, i.created_at, i.updated_at, i.closed_at FROM issues i",
        );
        let mut conditions = Vec::new();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        for (idx, label) in label_filters.iter().enumerate() {
            let alias = format!("l{}", idx);
            sql.push_str(&format!(
                " JOIN labels {} ON i.id = {}.issue_id AND {}.label = ?",
                alias, alias, alias
            ));
            params_vec.push(Box::new(label.clone()));
        }

        if let Some(status) = status_filter {
            if status != "all" {
                validate_status(status)?;
                conditions.push("i.status = ?".to_string());
                params_vec.push(Box::new(status.to_string()));
            }
        }

        if let Some(priority) = priority_filter {
            conditions.push("i.priority = ?".to_string());
            params_vec.push(Box::new(priority.to_string()));
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        sql.push_str(" ORDER BY i.id DESC");

        let mut stmt = self.conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();

        let issues = stmt
            .query_map(params_refs.as_slice(), issue_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(issues)
    }

    pub fn update_issue(
        &self,
        id: i64,
        title: Option<&str>,
        description: Option<&str>,
        priority: Option<&str>,
    ) -> Result<bool> {
        if let Some(t) = title {
            if t.len() > MAX_TITLE_LEN {
                anyhow::bail!(
                    "Title exceeds maximum length of {} characters",
                    MAX_TITLE_LEN
                );
            }
        }
        if let Some(d) = description {
            if d.len() > MAX_DESCRIPTION_LEN {
                anyhow::bail!(
                    "Description exceeds maximum length of {} bytes",
                    MAX_DESCRIPTION_LEN
                );
            }
        }
        if let Some(p) = priority {
            validate_priority(p)?;
        }

        let now = Utc::now().to_rfc3339();
        let mut updates = vec!["updated_at = ?1".to_string()];
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(now)];

        if let Some(t) = title {
            updates.push(format!("title = ?{}", params_vec.len() + 1));
            params_vec.push(Box::new(t.to_string()));
        }

        if let Some(d) = description {
            updates.push(format!("description = ?{}", params_vec.len() + 1));
            params_vec.push(Box::new(d.to_string()));
        }

        if let Some(p) = priority {
            updates.push(format!("priority = ?{}", params_vec.len() + 1));
            params_vec.push(Box::new(p.to_string()));
        }

        params_vec.push(Box::new(id));
        let sql = format!(
            "UPDATE issues SET {} WHERE id = ?{}",
            updates.join(", "),
            params_vec.len()
        );

        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let rows = self.conn.execute(&sql, params_refs.as_slice())?;
        Ok(rows > 0)
    }

    pub fn close_issue(&self, id: i64) -> Result<bool> {
        let now = Utc::now().to_rfc3339();
        let rows = self.conn.execute(
            "UPDATE issues SET status = 'closed', closed_at = ?1, updated_at = ?1 WHERE id = ?2",
            params![now, id],
        )?;
        Ok(rows > 0)
    }

    pub fn reopen_issue(&self, id: i64) -> Result<bool> {
        let now = Utc::now().to_rfc3339();
        let rows = self.conn.execute(
            "UPDATE issues SET status = 'open', closed_at = NULL, updated_at = ?1 WHERE id = ?2",
            params![now, id],
        )?;
        Ok(rows > 0)
    }

    pub fn delete_issue(&self, id: i64) -> Result<bool> {
        let rows = self
            .conn
            .execute("DELETE FROM issues WHERE id = ?1", [id])?;
        Ok(rows > 0)
    }

    pub fn update_parent(&self, id: i64, parent_id: Option<i64>) -> Result<bool> {
        let now = chrono::Utc::now().to_rfc3339();
        let rows = self.conn.execute(
            "UPDATE issues SET parent_id = ?1, updated_at = ?2 WHERE id = ?3",
            params![parent_id, now, id],
        )?;
        Ok(rows > 0)
    }

    /// Search issues by query string across titles, descriptions, and comments
    pub fn search_issues(&self, query: &str) -> Result<Vec<Issue>> {
        let escaped = query.replace('%', "\\%").replace('_', "\\_");
        let pattern = format!("%{}%", escaped);
        let mut stmt = self.conn.prepare(
            r#"
            SELECT DISTINCT i.id, i.title, i.description, i.status, i.priority, i.parent_id, i.created_at, i.updated_at, i.closed_at
            FROM issues i
            LEFT JOIN comments c ON i.id = c.issue_id
            WHERE i.title LIKE ?1 ESCAPE '\' COLLATE NOCASE
               OR i.description LIKE ?1 ESCAPE '\' COLLATE NOCASE
               OR c.content LIKE ?1 ESCAPE '\' COLLATE NOCASE
            ORDER BY i.id DESC
            "#,
        )?;

        let issues = stmt
            .query_map([&pattern], issue_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(issues)
    }
}
