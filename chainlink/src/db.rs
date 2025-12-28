use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use std::path::Path;

use crate::models::{Comment, Issue, Session};

const SCHEMA_VERSION: i32 = 1;

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path).context("Failed to open database")?;
        let db = Database { conn };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<()> {
        // Check if we need to initialize
        let version: i32 = self
            .conn
            .query_row(
                "SELECT COALESCE(MAX(version), 0) FROM pragma_user_version",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);

        if version < SCHEMA_VERSION {
            self.conn.execute_batch(
                r#"
                -- Core issues table
                CREATE TABLE IF NOT EXISTS issues (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT,
                    status TEXT NOT NULL DEFAULT 'open',
                    priority TEXT NOT NULL DEFAULT 'medium',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    closed_at TEXT
                );

                -- Labels (many-to-many)
                CREATE TABLE IF NOT EXISTS labels (
                    issue_id INTEGER NOT NULL,
                    label TEXT NOT NULL,
                    PRIMARY KEY (issue_id, label),
                    FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
                );

                -- Dependencies (blocker blocks blocked)
                CREATE TABLE IF NOT EXISTS dependencies (
                    blocker_id INTEGER NOT NULL,
                    blocked_id INTEGER NOT NULL,
                    PRIMARY KEY (blocker_id, blocked_id),
                    FOREIGN KEY (blocker_id) REFERENCES issues(id) ON DELETE CASCADE,
                    FOREIGN KEY (blocked_id) REFERENCES issues(id) ON DELETE CASCADE
                );

                -- Comments
                CREATE TABLE IF NOT EXISTS comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    issue_id INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
                );

                -- Sessions (for context preservation)
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    active_issue_id INTEGER,
                    handoff_notes TEXT,
                    FOREIGN KEY (active_issue_id) REFERENCES issues(id)
                );

                -- Indexes
                CREATE INDEX IF NOT EXISTS idx_issues_status ON issues(status);
                CREATE INDEX IF NOT EXISTS idx_issues_priority ON issues(priority);
                CREATE INDEX IF NOT EXISTS idx_labels_issue ON labels(issue_id);
                CREATE INDEX IF NOT EXISTS idx_comments_issue ON comments(issue_id);
                CREATE INDEX IF NOT EXISTS idx_deps_blocker ON dependencies(blocker_id);
                CREATE INDEX IF NOT EXISTS idx_deps_blocked ON dependencies(blocked_id);
                "#,
            )?;

            self.conn
                .execute(&format!("PRAGMA user_version = {}", SCHEMA_VERSION), [])?;
        }

        // Enable foreign keys
        self.conn.execute("PRAGMA foreign_keys = ON", [])?;

        Ok(())
    }

    // Issue CRUD
    pub fn create_issue(&self, title: &str, description: Option<&str>, priority: &str) -> Result<i64> {
        let now = Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO issues (title, description, priority, status, created_at, updated_at) VALUES (?1, ?2, ?3, 'open', ?4, ?4)",
            params![title, description, priority, now],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn get_issue(&self, id: i64) -> Result<Option<Issue>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, title, description, status, priority, created_at, updated_at, closed_at FROM issues WHERE id = ?1",
        )?;

        let issue = stmt
            .query_row([id], |row| {
                Ok(Issue {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    description: row.get(2)?,
                    status: row.get(3)?,
                    priority: row.get(4)?,
                    created_at: parse_datetime(row.get::<_, String>(5)?),
                    updated_at: parse_datetime(row.get::<_, String>(6)?),
                    closed_at: row.get::<_, Option<String>>(7)?.map(parse_datetime),
                })
            })
            .ok();

        Ok(issue)
    }

    pub fn list_issues(
        &self,
        status_filter: Option<&str>,
        label_filter: Option<&str>,
        priority_filter: Option<&str>,
    ) -> Result<Vec<Issue>> {
        let mut sql = String::from(
            "SELECT DISTINCT i.id, i.title, i.description, i.status, i.priority, i.created_at, i.updated_at, i.closed_at FROM issues i",
        );
        let mut conditions = Vec::new();
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if label_filter.is_some() {
            sql.push_str(" JOIN labels l ON i.id = l.issue_id");
        }

        if let Some(status) = status_filter {
            if status != "all" {
                conditions.push("i.status = ?".to_string());
                params_vec.push(Box::new(status.to_string()));
            }
        }

        if let Some(label) = label_filter {
            conditions.push("l.label = ?".to_string());
            params_vec.push(Box::new(label.to_string()));
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
        let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|p| p.as_ref()).collect();

        let issues = stmt
            .query_map(params_refs.as_slice(), |row| {
                Ok(Issue {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    description: row.get(2)?,
                    status: row.get(3)?,
                    priority: row.get(4)?,
                    created_at: parse_datetime(row.get::<_, String>(5)?),
                    updated_at: parse_datetime(row.get::<_, String>(6)?),
                    closed_at: row.get::<_, Option<String>>(7)?.map(parse_datetime),
                })
            })?
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

        let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|p| p.as_ref()).collect();
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
        let rows = self.conn.execute("DELETE FROM issues WHERE id = ?1", [id])?;
        Ok(rows > 0)
    }

    // Labels
    pub fn add_label(&self, issue_id: i64, label: &str) -> Result<bool> {
        let result = self.conn.execute(
            "INSERT OR IGNORE INTO labels (issue_id, label) VALUES (?1, ?2)",
            params![issue_id, label],
        )?;
        Ok(result > 0)
    }

    pub fn remove_label(&self, issue_id: i64, label: &str) -> Result<bool> {
        let rows = self.conn.execute(
            "DELETE FROM labels WHERE issue_id = ?1 AND label = ?2",
            params![issue_id, label],
        )?;
        Ok(rows > 0)
    }

    pub fn get_labels(&self, issue_id: i64) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT label FROM labels WHERE issue_id = ?1 ORDER BY label")?;
        let labels = stmt
            .query_map([issue_id], |row| row.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;
        Ok(labels)
    }

    // Comments
    pub fn add_comment(&self, issue_id: i64, content: &str) -> Result<i64> {
        let now = Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO comments (issue_id, content, created_at) VALUES (?1, ?2, ?3)",
            params![issue_id, content, now],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn get_comments(&self, issue_id: i64) -> Result<Vec<Comment>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, issue_id, content, created_at FROM comments WHERE issue_id = ?1 ORDER BY created_at",
        )?;
        let comments = stmt
            .query_map([issue_id], |row| {
                Ok(Comment {
                    id: row.get(0)?,
                    issue_id: row.get(1)?,
                    content: row.get(2)?,
                    created_at: parse_datetime(row.get::<_, String>(3)?),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(comments)
    }

    // Dependencies
    pub fn add_dependency(&self, blocked_id: i64, blocker_id: i64) -> Result<bool> {
        let result = self.conn.execute(
            "INSERT OR IGNORE INTO dependencies (blocker_id, blocked_id) VALUES (?1, ?2)",
            params![blocker_id, blocked_id],
        )?;
        Ok(result > 0)
    }

    pub fn remove_dependency(&self, blocked_id: i64, blocker_id: i64) -> Result<bool> {
        let rows = self.conn.execute(
            "DELETE FROM dependencies WHERE blocker_id = ?1 AND blocked_id = ?2",
            params![blocker_id, blocked_id],
        )?;
        Ok(rows > 0)
    }

    pub fn get_blockers(&self, issue_id: i64) -> Result<Vec<i64>> {
        let mut stmt = self
            .conn
            .prepare("SELECT blocker_id FROM dependencies WHERE blocked_id = ?1")?;
        let blockers = stmt
            .query_map([issue_id], |row| row.get(0))?
            .collect::<std::result::Result<Vec<i64>, _>>()?;
        Ok(blockers)
    }

    pub fn get_blocking(&self, issue_id: i64) -> Result<Vec<i64>> {
        let mut stmt = self
            .conn
            .prepare("SELECT blocked_id FROM dependencies WHERE blocker_id = ?1")?;
        let blocking = stmt
            .query_map([issue_id], |row| row.get(0))?
            .collect::<std::result::Result<Vec<i64>, _>>()?;
        Ok(blocking)
    }

    pub fn list_blocked_issues(&self) -> Result<Vec<Issue>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT DISTINCT i.id, i.title, i.description, i.status, i.priority, i.created_at, i.updated_at, i.closed_at
            FROM issues i
            JOIN dependencies d ON i.id = d.blocked_id
            JOIN issues blocker ON d.blocker_id = blocker.id
            WHERE i.status = 'open' AND blocker.status = 'open'
            ORDER BY i.id
            "#,
        )?;

        let issues = stmt
            .query_map([], |row| {
                Ok(Issue {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    description: row.get(2)?,
                    status: row.get(3)?,
                    priority: row.get(4)?,
                    created_at: parse_datetime(row.get::<_, String>(5)?),
                    updated_at: parse_datetime(row.get::<_, String>(6)?),
                    closed_at: row.get::<_, Option<String>>(7)?.map(parse_datetime),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(issues)
    }

    pub fn list_ready_issues(&self) -> Result<Vec<Issue>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT i.id, i.title, i.description, i.status, i.priority, i.created_at, i.updated_at, i.closed_at
            FROM issues i
            WHERE i.status = 'open'
            AND NOT EXISTS (
                SELECT 1 FROM dependencies d
                JOIN issues blocker ON d.blocker_id = blocker.id
                WHERE d.blocked_id = i.id AND blocker.status = 'open'
            )
            ORDER BY i.id
            "#,
        )?;

        let issues = stmt
            .query_map([], |row| {
                Ok(Issue {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    description: row.get(2)?,
                    status: row.get(3)?,
                    priority: row.get(4)?,
                    created_at: parse_datetime(row.get::<_, String>(5)?),
                    updated_at: parse_datetime(row.get::<_, String>(6)?),
                    closed_at: row.get::<_, Option<String>>(7)?.map(parse_datetime),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(issues)
    }

    // Sessions
    pub fn start_session(&self) -> Result<i64> {
        let now = Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO sessions (started_at) VALUES (?1)",
            params![now],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn end_session(&self, id: i64, notes: Option<&str>) -> Result<bool> {
        let now = Utc::now().to_rfc3339();
        let rows = self.conn.execute(
            "UPDATE sessions SET ended_at = ?1, handoff_notes = ?2 WHERE id = ?3",
            params![now, notes, id],
        )?;
        Ok(rows > 0)
    }

    pub fn get_current_session(&self) -> Result<Option<Session>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, started_at, ended_at, active_issue_id, handoff_notes FROM sessions WHERE ended_at IS NULL ORDER BY id DESC LIMIT 1",
        )?;

        let session = stmt
            .query_row([], |row| {
                Ok(Session {
                    id: row.get(0)?,
                    started_at: parse_datetime(row.get::<_, String>(1)?),
                    ended_at: row.get::<_, Option<String>>(2)?.map(parse_datetime),
                    active_issue_id: row.get(3)?,
                    handoff_notes: row.get(4)?,
                })
            })
            .ok();

        Ok(session)
    }

    pub fn get_last_session(&self) -> Result<Option<Session>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, started_at, ended_at, active_issue_id, handoff_notes FROM sessions WHERE ended_at IS NOT NULL ORDER BY id DESC LIMIT 1",
        )?;

        let session = stmt
            .query_row([], |row| {
                Ok(Session {
                    id: row.get(0)?,
                    started_at: parse_datetime(row.get::<_, String>(1)?),
                    ended_at: row.get::<_, Option<String>>(2)?.map(parse_datetime),
                    active_issue_id: row.get(3)?,
                    handoff_notes: row.get(4)?,
                })
            })
            .ok();

        Ok(session)
    }

    pub fn set_session_issue(&self, session_id: i64, issue_id: i64) -> Result<bool> {
        let rows = self.conn.execute(
            "UPDATE sessions SET active_issue_id = ?1 WHERE id = ?2",
            params![issue_id, session_id],
        )?;
        Ok(rows > 0)
    }
}

fn parse_datetime(s: String) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(&s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}
