use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

use crate::db::Database;

// Embed hook files at compile time
// Path: chainlink/src/commands/init.rs -> ../../../.claude/
const SETTINGS_JSON: &str = include_str!("../../../.claude/settings.json");
const PROMPT_GUARD_PY: &str = include_str!("../../../.claude/hooks/prompt-guard.py");
const POST_EDIT_CHECK_PY: &str = include_str!("../../../.claude/hooks/post-edit-check.py");
const SESSION_START_PY: &str = include_str!("../../../.claude/hooks/session-start.py");

pub fn run(path: &Path, force: bool) -> Result<()> {
    let chainlink_dir = path.join(".chainlink");
    let claude_dir = path.join(".claude");
    let hooks_dir = claude_dir.join("hooks");

    // Check if already initialized
    let chainlink_exists = chainlink_dir.exists();
    let claude_exists = claude_dir.exists();

    if chainlink_exists && claude_exists && !force {
        println!("Already initialized at {}", path.display());
        println!("Use --force to update hooks to latest version.");
        return Ok(());
    }

    // Create .chainlink directory and database
    if !chainlink_exists {
        fs::create_dir_all(&chainlink_dir)
            .context("Failed to create .chainlink directory")?;

        let db_path = chainlink_dir.join("issues.db");
        Database::open(&db_path)?;
        println!("Created {}", chainlink_dir.display());
    }

    // Create .claude directory and hooks (or update if force)
    if !claude_exists || force {
        fs::create_dir_all(&hooks_dir)
            .context("Failed to create .claude/hooks directory")?;

        // Write settings.json
        fs::write(claude_dir.join("settings.json"), SETTINGS_JSON)
            .context("Failed to write settings.json")?;

        // Write hook scripts
        fs::write(hooks_dir.join("prompt-guard.py"), PROMPT_GUARD_PY)
            .context("Failed to write prompt-guard.py")?;

        fs::write(hooks_dir.join("post-edit-check.py"), POST_EDIT_CHECK_PY)
            .context("Failed to write post-edit-check.py")?;

        fs::write(hooks_dir.join("session-start.py"), SESSION_START_PY)
            .context("Failed to write session-start.py")?;

        if force && claude_exists {
            println!("Updated {} with latest hooks", claude_dir.display());
        } else {
            println!("Created {} with Claude Code hooks", claude_dir.display());
        }
    }

    println!("Chainlink initialized successfully!");
    println!("\nNext steps:");
    println!("  chainlink session start     # Start a session");
    println!("  chainlink create \"Task\"     # Create an issue");

    Ok(())
}
