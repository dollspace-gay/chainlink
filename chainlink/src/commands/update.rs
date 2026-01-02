use anyhow::{bail, Result};

use crate::commands::create::validate_priority;
use crate::db::Database;

pub fn run(
    db: &Database,
    id: i64,
    title: Option<&str>,
    description: Option<&str>,
    priority: Option<&str>,
) -> Result<()> {
    if title.is_none() && description.is_none() && priority.is_none() {
        bail!("Nothing to update. Use --title, --description, or --priority");
    }

    if let Some(p) = priority {
        if !validate_priority(p) {
            bail!(
                "Invalid priority '{}'. Must be one of: low, medium, high, critical",
                p
            );
        }
    }

    if db.update_issue(id, title, description, priority)? {
        println!("Updated issue #{}", id);
    } else {
        bail!("Issue #{} not found", id);
    }

    Ok(())
}
