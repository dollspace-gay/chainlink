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

// Embed rule files at compile time
// Path: chainlink/src/commands/init.rs -> ../../../.chainlink/rules/
const RULE_GLOBAL: &str = include_str!("../../../.chainlink/rules/global.md");
const RULE_PROJECT: &str = include_str!("../../../.chainlink/rules/project.md");
const RULE_RUST: &str = include_str!("../../../.chainlink/rules/rust.md");
const RULE_PYTHON: &str = include_str!("../../../.chainlink/rules/python.md");
const RULE_JAVASCRIPT: &str = include_str!("../../../.chainlink/rules/javascript.md");
const RULE_TYPESCRIPT: &str = include_str!("../../../.chainlink/rules/typescript.md");
const RULE_TYPESCRIPT_REACT: &str = include_str!("../../../.chainlink/rules/typescript-react.md");
const RULE_JAVASCRIPT_REACT: &str = include_str!("../../../.chainlink/rules/javascript-react.md");
const RULE_GO: &str = include_str!("../../../.chainlink/rules/go.md");
const RULE_JAVA: &str = include_str!("../../../.chainlink/rules/java.md");
const RULE_C: &str = include_str!("../../../.chainlink/rules/c.md");
const RULE_CPP: &str = include_str!("../../../.chainlink/rules/cpp.md");
const RULE_CSHARP: &str = include_str!("../../../.chainlink/rules/csharp.md");
const RULE_RUBY: &str = include_str!("../../../.chainlink/rules/ruby.md");
const RULE_PHP: &str = include_str!("../../../.chainlink/rules/php.md");
const RULE_SWIFT: &str = include_str!("../../../.chainlink/rules/swift.md");
const RULE_KOTLIN: &str = include_str!("../../../.chainlink/rules/kotlin.md");
const RULE_SCALA: &str = include_str!("../../../.chainlink/rules/scala.md");
const RULE_ZIG: &str = include_str!("../../../.chainlink/rules/zig.md");
const RULE_ODIN: &str = include_str!("../../../.chainlink/rules/odin.md");

/// All rule files to deploy
const RULE_FILES: &[(&str, &str)] = &[
    ("global.md", RULE_GLOBAL),
    ("project.md", RULE_PROJECT),
    ("rust.md", RULE_RUST),
    ("python.md", RULE_PYTHON),
    ("javascript.md", RULE_JAVASCRIPT),
    ("typescript.md", RULE_TYPESCRIPT),
    ("typescript-react.md", RULE_TYPESCRIPT_REACT),
    ("javascript-react.md", RULE_JAVASCRIPT_REACT),
    ("go.md", RULE_GO),
    ("java.md", RULE_JAVA),
    ("c.md", RULE_C),
    ("cpp.md", RULE_CPP),
    ("csharp.md", RULE_CSHARP),
    ("ruby.md", RULE_RUBY),
    ("php.md", RULE_PHP),
    ("swift.md", RULE_SWIFT),
    ("kotlin.md", RULE_KOTLIN),
    ("scala.md", RULE_SCALA),
    ("zig.md", RULE_ZIG),
    ("odin.md", RULE_ODIN),
];

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

    let rules_dir = chainlink_dir.join("rules");

    // Create .chainlink directory and database
    if !chainlink_exists {
        fs::create_dir_all(&chainlink_dir).context("Failed to create .chainlink directory")?;

        let db_path = chainlink_dir.join("issues.db");
        Database::open(&db_path)?;
        println!("Created {}", chainlink_dir.display());
    }

    // Create or update rules directory
    let rules_exist = rules_dir.exists();
    if !rules_exist || force {
        fs::create_dir_all(&rules_dir).context("Failed to create .chainlink/rules directory")?;

        for (filename, content) in RULE_FILES {
            fs::write(rules_dir.join(filename), content)
                .with_context(|| format!("Failed to write {}", filename))?;
        }

        if force && rules_exist {
            println!("Updated {} with latest rules", rules_dir.display());
        } else {
            println!("Created {} with default rules", rules_dir.display());
        }
    }

    // Create .claude directory and hooks (or update if force)
    if !claude_exists || force {
        fs::create_dir_all(&hooks_dir).context("Failed to create .claude/hooks directory")?;

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
