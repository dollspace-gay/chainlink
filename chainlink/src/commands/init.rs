use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

use crate::db::Database;

pub fn run(path: &Path) -> Result<()> {
    let chainlink_dir = path.join(".chainlink");

    if chainlink_dir.exists() {
        println!("Already initialized at {}", chainlink_dir.display());
        return Ok(());
    }

    fs::create_dir_all(&chainlink_dir)
        .context("Failed to create .chainlink directory")?;

    let db_path = chainlink_dir.join("issues.db");
    Database::open(&db_path)?;

    println!("Initialized chainlink in {}", chainlink_dir.display());
    Ok(())
}
