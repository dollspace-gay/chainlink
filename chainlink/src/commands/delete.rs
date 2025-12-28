use anyhow::{bail, Result};
use std::io::{self, Write};

use crate::db::Database;

pub fn run(db: &Database, id: i64, force: bool) -> Result<()> {
    // Check if issue exists first
    let issue = match db.get_issue(id)? {
        Some(i) => i,
        None => bail!("Issue #{} not found", id),
    };

    if !force {
        print!("Delete issue #{} \"{}\"? [y/N] ", id, issue.title);
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Cancelled.");
            return Ok(());
        }
    }

    if db.delete_issue(id)? {
        println!("Deleted issue #{}", id);
    } else {
        bail!("Failed to delete issue #{}", id);
    }

    Ok(())
}
