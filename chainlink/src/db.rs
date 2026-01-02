use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use std::path::Path;

use crate::models::{Comment, Issue, Session};

const SCHEMA_VERSION: i32 = 6;

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
                    parent_id INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    closed_at TEXT,
                    FOREIGN KEY (parent_id) REFERENCES issues(id) ON DELETE CASCADE
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

                -- Time tracking
                CREATE TABLE IF NOT EXISTS time_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    issue_id INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    duration_seconds INTEGER,
                    FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
                );

                -- Relations (related issues, bidirectional)
                CREATE TABLE IF NOT EXISTS relations (
                    issue_id_1 INTEGER NOT NULL,
                    issue_id_2 INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (issue_id_1, issue_id_2),
                    FOREIGN KEY (issue_id_1) REFERENCES issues(id) ON DELETE CASCADE,
                    FOREIGN KEY (issue_id_2) REFERENCES issues(id) ON DELETE CASCADE
                );

                -- Milestones
                CREATE TABLE IF NOT EXISTS milestones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    status TEXT NOT NULL DEFAULT 'open',
                    created_at TEXT NOT NULL,
                    closed_at TEXT
                );

                -- Milestone-Issue relationship (many-to-many)
                CREATE TABLE IF NOT EXISTS milestone_issues (
                    milestone_id INTEGER NOT NULL,
                    issue_id INTEGER NOT NULL,
                    PRIMARY KEY (milestone_id, issue_id),
                    FOREIGN KEY (milestone_id) REFERENCES milestones(id) ON DELETE CASCADE,
                    FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
                );

                -- Indexes
                CREATE INDEX IF NOT EXISTS idx_issues_status ON issues(status);
                CREATE INDEX IF NOT EXISTS idx_issues_priority ON issues(priority);
                CREATE INDEX IF NOT EXISTS idx_labels_issue ON labels(issue_id);
                CREATE INDEX IF NOT EXISTS idx_comments_issue ON comments(issue_id);
                CREATE INDEX IF NOT EXISTS idx_deps_blocker ON dependencies(blocker_id);
                CREATE INDEX IF NOT EXISTS idx_deps_blocked ON dependencies(blocked_id);
                CREATE INDEX IF NOT EXISTS idx_issues_parent ON issues(parent_id);
                CREATE INDEX IF NOT EXISTS idx_time_entries_issue ON time_entries(issue_id);
                CREATE INDEX IF NOT EXISTS idx_relations_1 ON relations(issue_id_1);
                CREATE INDEX IF NOT EXISTS idx_relations_2 ON relations(issue_id_2);
                CREATE INDEX IF NOT EXISTS idx_milestone_issues_m ON milestone_issues(milestone_id);
                CREATE INDEX IF NOT EXISTS idx_milestone_issues_i ON milestone_issues(issue_id);
                "#,
            )?;

            // Migration: add parent_id column if upgrading from v1
            let _ = self.conn.execute(
                "ALTER TABLE issues ADD COLUMN parent_id INTEGER REFERENCES issues(id) ON DELETE CASCADE",
                [],
            );

            self.conn
                .execute(&format!("PRAGMA user_version = {}", SCHEMA_VERSION), [])?;
        }

        // Enable foreign keys
        self.conn.execute("PRAGMA foreign_keys = ON", [])?;

        Ok(())
    }

    // Issue CRUD
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
            .query_map([parent_id], |row| {
                Ok(Issue {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    description: row.get(2)?,
                    status: row.get(3)?,
                    priority: row.get(4)?,
                    parent_id: row.get(5)?,
                    created_at: parse_datetime(row.get::<_, String>(6)?),
                    updated_at: parse_datetime(row.get::<_, String>(7)?),
                    closed_at: row.get::<_, Option<String>>(8)?.map(parse_datetime),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(issues)
    }

    pub fn get_issue(&self, id: i64) -> Result<Option<Issue>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, title, description, status, priority, parent_id, created_at, updated_at, closed_at FROM issues WHERE id = ?1",
        )?;

        let issue = stmt
            .query_row([id], |row| {
                Ok(Issue {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    description: row.get(2)?,
                    status: row.get(3)?,
                    priority: row.get(4)?,
                    parent_id: row.get(5)?,
                    created_at: parse_datetime(row.get::<_, String>(6)?),
                    updated_at: parse_datetime(row.get::<_, String>(7)?),
                    closed_at: row.get::<_, Option<String>>(8)?.map(parse_datetime),
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
            "SELECT DISTINCT i.id, i.title, i.description, i.status, i.priority, i.parent_id, i.created_at, i.updated_at, i.closed_at FROM issues i",
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
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();

        let issues = stmt
            .query_map(params_refs.as_slice(), |row| {
                Ok(Issue {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    description: row.get(2)?,
                    status: row.get(3)?,
                    priority: row.get(4)?,
                    parent_id: row.get(5)?,
                    created_at: parse_datetime(row.get::<_, String>(6)?),
                    updated_at: parse_datetime(row.get::<_, String>(7)?),
                    closed_at: row.get::<_, Option<String>>(8)?.map(parse_datetime),
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
            SELECT DISTINCT i.id, i.title, i.description, i.status, i.priority, i.parent_id, i.created_at, i.updated_at, i.closed_at
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
                    parent_id: row.get(5)?,
                    created_at: parse_datetime(row.get::<_, String>(6)?),
                    updated_at: parse_datetime(row.get::<_, String>(7)?),
                    closed_at: row.get::<_, Option<String>>(8)?.map(parse_datetime),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(issues)
    }

    pub fn list_ready_issues(&self) -> Result<Vec<Issue>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT i.id, i.title, i.description, i.status, i.priority, i.parent_id, i.created_at, i.updated_at, i.closed_at
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
                    parent_id: row.get(5)?,
                    created_at: parse_datetime(row.get::<_, String>(6)?),
                    updated_at: parse_datetime(row.get::<_, String>(7)?),
                    closed_at: row.get::<_, Option<String>>(8)?.map(parse_datetime),
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

    // Time tracking
    pub fn start_timer(&self, issue_id: i64) -> Result<i64> {
        let now = Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO time_entries (issue_id, started_at) VALUES (?1, ?2)",
            params![issue_id, now],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn stop_timer(&self, issue_id: i64) -> Result<bool> {
        let now = Utc::now();
        let now_str = now.to_rfc3339();

        // Get the active entry
        let started_at: Option<String> = self
            .conn
            .query_row(
                "SELECT started_at FROM time_entries WHERE issue_id = ?1 AND ended_at IS NULL",
                [issue_id],
                |row| row.get(0),
            )
            .ok();

        if let Some(started) = started_at {
            let start_dt = DateTime::parse_from_rfc3339(&started)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or(now);
            let duration = now.signed_duration_since(start_dt).num_seconds();

            let rows = self.conn.execute(
                "UPDATE time_entries SET ended_at = ?1, duration_seconds = ?2 WHERE issue_id = ?3 AND ended_at IS NULL",
                params![now_str, duration, issue_id],
            )?;
            Ok(rows > 0)
        } else {
            Ok(false)
        }
    }

    pub fn get_active_timer(&self) -> Result<Option<(i64, DateTime<Utc>)>> {
        let result: Option<(i64, String)> = self
            .conn
            .query_row(
                "SELECT issue_id, started_at FROM time_entries WHERE ended_at IS NULL ORDER BY id DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();

        Ok(result.map(|(id, started)| (id, parse_datetime(started))))
    }

    pub fn get_total_time(&self, issue_id: i64) -> Result<i64> {
        let total: i64 = self
            .conn
            .query_row(
                "SELECT COALESCE(SUM(duration_seconds), 0) FROM time_entries WHERE issue_id = ?1 AND duration_seconds IS NOT NULL",
                [issue_id],
                |row| row.get(0),
            )
            .unwrap_or(0);
        Ok(total)
    }

    /// Search issues by query string across titles, descriptions, and comments
    pub fn search_issues(&self, query: &str) -> Result<Vec<Issue>> {
        let pattern = format!("%{}%", query);
        let mut stmt = self.conn.prepare(
            r#"
            SELECT DISTINCT i.id, i.title, i.description, i.status, i.priority, i.parent_id, i.created_at, i.updated_at, i.closed_at
            FROM issues i
            LEFT JOIN comments c ON i.id = c.issue_id
            WHERE i.title LIKE ?1 COLLATE NOCASE
               OR i.description LIKE ?1 COLLATE NOCASE
               OR c.content LIKE ?1 COLLATE NOCASE
            ORDER BY i.id DESC
            "#,
        )?;

        let issues = stmt
            .query_map([&pattern], |row| {
                Ok(Issue {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    description: row.get(2)?,
                    status: row.get(3)?,
                    priority: row.get(4)?,
                    parent_id: row.get(5)?,
                    created_at: parse_datetime(row.get::<_, String>(6)?),
                    updated_at: parse_datetime(row.get::<_, String>(7)?),
                    closed_at: row.get::<_, Option<String>>(8)?.map(parse_datetime),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(issues)
    }

    // Relations (bidirectional)
    pub fn add_relation(&self, issue_id_1: i64, issue_id_2: i64) -> Result<bool> {
        if issue_id_1 == issue_id_2 {
            anyhow::bail!("Cannot relate an issue to itself");
        }
        // Store with smaller ID first for consistency
        let (a, b) = if issue_id_1 < issue_id_2 {
            (issue_id_1, issue_id_2)
        } else {
            (issue_id_2, issue_id_1)
        };
        let now = Utc::now().to_rfc3339();
        let result = self.conn.execute(
            "INSERT OR IGNORE INTO relations (issue_id_1, issue_id_2, created_at) VALUES (?1, ?2, ?3)",
            params![a, b, now],
        )?;
        Ok(result > 0)
    }

    pub fn remove_relation(&self, issue_id_1: i64, issue_id_2: i64) -> Result<bool> {
        let (a, b) = if issue_id_1 < issue_id_2 {
            (issue_id_1, issue_id_2)
        } else {
            (issue_id_2, issue_id_1)
        };
        let rows = self.conn.execute(
            "DELETE FROM relations WHERE issue_id_1 = ?1 AND issue_id_2 = ?2",
            params![a, b],
        )?;
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

    pub fn get_related_issues(&self, issue_id: i64) -> Result<Vec<Issue>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT i.id, i.title, i.description, i.status, i.priority, i.parent_id, i.created_at, i.updated_at, i.closed_at
            FROM issues i
            WHERE i.id IN (
                SELECT issue_id_2 FROM relations WHERE issue_id_1 = ?1
                UNION
                SELECT issue_id_1 FROM relations WHERE issue_id_2 = ?1
            )
            ORDER BY i.id
            "#,
        )?;

        let issues = stmt
            .query_map([issue_id], |row| {
                Ok(Issue {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    description: row.get(2)?,
                    status: row.get(3)?,
                    priority: row.get(4)?,
                    parent_id: row.get(5)?,
                    created_at: parse_datetime(row.get::<_, String>(6)?),
                    updated_at: parse_datetime(row.get::<_, String>(7)?),
                    closed_at: row.get::<_, Option<String>>(8)?.map(parse_datetime),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(issues)
    }

    // Milestones
    pub fn create_milestone(&self, name: &str, description: Option<&str>) -> Result<i64> {
        let now = Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO milestones (name, description, status, created_at) VALUES (?1, ?2, 'open', ?3)",
            params![name, description, now],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn get_milestone(&self, id: i64) -> Result<Option<crate::models::Milestone>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, description, status, created_at, closed_at FROM milestones WHERE id = ?1",
        )?;

        let milestone = stmt
            .query_row([id], |row| {
                Ok(crate::models::Milestone {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    status: row.get(3)?,
                    created_at: parse_datetime(row.get::<_, String>(4)?),
                    closed_at: row.get::<_, Option<String>>(5)?.map(parse_datetime),
                })
            })
            .ok();

        Ok(milestone)
    }

    pub fn list_milestones(&self, status: Option<&str>) -> Result<Vec<crate::models::Milestone>> {
        let sql = if let Some(s) = status {
            if s == "all" {
                "SELECT id, name, description, status, created_at, closed_at FROM milestones ORDER BY id DESC".to_string()
            } else {
                format!("SELECT id, name, description, status, created_at, closed_at FROM milestones WHERE status = '{}' ORDER BY id DESC", s)
            }
        } else {
            "SELECT id, name, description, status, created_at, closed_at FROM milestones WHERE status = 'open' ORDER BY id DESC".to_string()
        };

        let mut stmt = self.conn.prepare(&sql)?;
        let milestones = stmt
            .query_map([], |row| {
                Ok(crate::models::Milestone {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    status: row.get(3)?,
                    created_at: parse_datetime(row.get::<_, String>(4)?),
                    closed_at: row.get::<_, Option<String>>(5)?.map(parse_datetime),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(milestones)
    }

    pub fn add_issue_to_milestone(&self, milestone_id: i64, issue_id: i64) -> Result<bool> {
        let result = self.conn.execute(
            "INSERT OR IGNORE INTO milestone_issues (milestone_id, issue_id) VALUES (?1, ?2)",
            params![milestone_id, issue_id],
        )?;
        Ok(result > 0)
    }

    pub fn remove_issue_from_milestone(&self, milestone_id: i64, issue_id: i64) -> Result<bool> {
        let rows = self.conn.execute(
            "DELETE FROM milestone_issues WHERE milestone_id = ?1 AND issue_id = ?2",
            params![milestone_id, issue_id],
        )?;
        Ok(rows > 0)
    }

    pub fn get_milestone_issues(&self, milestone_id: i64) -> Result<Vec<Issue>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT i.id, i.title, i.description, i.status, i.priority, i.parent_id, i.created_at, i.updated_at, i.closed_at
            FROM issues i
            JOIN milestone_issues mi ON i.id = mi.issue_id
            WHERE mi.milestone_id = ?1
            ORDER BY i.id
            "#,
        )?;

        let issues = stmt
            .query_map([milestone_id], |row| {
                Ok(Issue {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    description: row.get(2)?,
                    status: row.get(3)?,
                    priority: row.get(4)?,
                    parent_id: row.get(5)?,
                    created_at: parse_datetime(row.get::<_, String>(6)?),
                    updated_at: parse_datetime(row.get::<_, String>(7)?),
                    closed_at: row.get::<_, Option<String>>(8)?.map(parse_datetime),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(issues)
    }

    pub fn close_milestone(&self, id: i64) -> Result<bool> {
        let now = Utc::now().to_rfc3339();
        let rows = self.conn.execute(
            "UPDATE milestones SET status = 'closed', closed_at = ?1 WHERE id = ?2",
            params![now, id],
        )?;
        Ok(rows > 0)
    }

    pub fn delete_milestone(&self, id: i64) -> Result<bool> {
        let rows = self
            .conn
            .execute("DELETE FROM milestones WHERE id = ?1", [id])?;
        Ok(rows > 0)
    }

    pub fn get_issue_milestone(&self, issue_id: i64) -> Result<Option<crate::models::Milestone>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT m.id, m.name, m.description, m.status, m.created_at, m.closed_at
            FROM milestones m
            JOIN milestone_issues mi ON m.id = mi.milestone_id
            WHERE mi.issue_id = ?1
            LIMIT 1
            "#,
        )?;

        let milestone = stmt
            .query_row([issue_id], |row| {
                Ok(crate::models::Milestone {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    description: row.get(2)?,
                    status: row.get(3)?,
                    created_at: parse_datetime(row.get::<_, String>(4)?),
                    closed_at: row.get::<_, Option<String>>(5)?.map(parse_datetime),
                })
            })
            .ok();

        Ok(milestone)
    }

    // Archiving
    pub fn archive_issue(&self, id: i64) -> Result<bool> {
        let now = Utc::now().to_rfc3339();
        let rows = self.conn.execute(
            "UPDATE issues SET status = 'archived', updated_at = ?1 WHERE id = ?2 AND status = 'closed'",
            params![now, id],
        )?;
        Ok(rows > 0)
    }

    pub fn unarchive_issue(&self, id: i64) -> Result<bool> {
        let now = Utc::now().to_rfc3339();
        let rows = self.conn.execute(
            "UPDATE issues SET status = 'closed', updated_at = ?1 WHERE id = ?2 AND status = 'archived'",
            params![now, id],
        )?;
        Ok(rows > 0)
    }

    pub fn list_archived_issues(&self) -> Result<Vec<Issue>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, title, description, status, priority, parent_id, created_at, updated_at, closed_at FROM issues WHERE status = 'archived' ORDER BY id DESC",
        )?;

        let issues = stmt
            .query_map([], |row| {
                Ok(Issue {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    description: row.get(2)?,
                    status: row.get(3)?,
                    priority: row.get(4)?,
                    parent_id: row.get(5)?,
                    created_at: parse_datetime(row.get::<_, String>(6)?),
                    updated_at: parse_datetime(row.get::<_, String>(7)?),
                    closed_at: row.get::<_, Option<String>>(8)?.map(parse_datetime),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(issues)
    }

    pub fn archive_older_than(&self, days: i64) -> Result<i32> {
        let cutoff = Utc::now() - chrono::Duration::days(days);
        let cutoff_str = cutoff.to_rfc3339();
        let now = Utc::now().to_rfc3339();

        let rows = self.conn.execute(
            "UPDATE issues SET status = 'archived', updated_at = ?1 WHERE status = 'closed' AND closed_at < ?2",
            params![now, cutoff_str],
        )?;

        Ok(rows as i32)
    }
}

fn parse_datetime(s: String) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(&s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}
