use anyhow::{bail, Result};

use crate::db::Database;

const VALID_PRIORITIES: [&str; 4] = ["low", "medium", "high", "critical"];

pub fn validate_priority(priority: &str) -> bool {
    VALID_PRIORITIES.contains(&priority)
}

pub fn run(db: &Database, title: &str, description: Option<&str>, priority: &str) -> Result<()> {
    if !validate_priority(priority) {
        bail!(
            "Invalid priority '{}'. Must be one of: {}",
            priority,
            VALID_PRIORITIES.join(", ")
        );
    }

    let id = db.create_issue(title, description, priority)?;
    println!("Created issue #{}", id);
    Ok(())
}
