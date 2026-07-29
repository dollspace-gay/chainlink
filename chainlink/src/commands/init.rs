use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

use crate::db::Database;

// Embed hook files at compile time from resources/ (packaged with the crate)
const SETTINGS_JSON: &str = include_str!("../../resources/claude/settings.json");
const PROMPT_GUARD_PY: &str = include_str!("../../resources/claude/hooks/prompt-guard.py");
const POST_EDIT_CHECK_PY: &str = include_str!("../../resources/claude/hooks/post-edit-check.py");
const SESSION_START_PY: &str = include_str!("../../resources/claude/hooks/session-start.py");
const PRE_WEB_CHECK_PY: &str = include_str!("../../resources/claude/hooks/pre-web-check.py");
const WORK_CHECK_PY: &str = include_str!("../../resources/claude/hooks/work-check.py");
const CHAINLINK_CONFIG_PY: &str = include_str!("../../resources/claude/hooks/chainlink_config.py");

// Embed MCP server for safe web fetching
const SAFE_FETCH_SERVER_PY: &str = include_str!("../../resources/claude/mcp/safe-fetch-server.py");
const MCP_JSON: &str = include_str!("../../resources/mcp.json");

// Embed hook configuration
const HOOK_CONFIG_JSON: &str = include_str!("../../resources/chainlink/hook-config.json");

// Auto-generated rule file includes from build.rs
// Includes all RULE_* constants and RULE_FILES array
// Adding a new rule = just drop a .md/.txt file in resources/chainlink/rules/
include!(concat!(env!("OUT_DIR"), "/rules_gen.rs"));

/// Merge chainlink's MCP server entries into an existing `.mcp.json`, or create it fresh.
/// Returns a list of warnings (e.g. overwritten keys) for the caller to display.
fn write_mcp_json_merged(mcp_path: &Path) -> Result<Vec<String>> {
    let embedded: serde_json::Value = serde_json::from_str(MCP_JSON)
        .context("embedded MCP_JSON is not valid JSON — this is a build defect")?;
    let src_servers = embedded
        .get("mcpServers")
        .and_then(|v| v.as_object())
        .context("embedded MCP_JSON missing mcpServers object — this is a build defect")?;

    let mut obj = match fs::read_to_string(mcp_path) {
        Ok(raw) => {
            let parsed: serde_json::Value = serde_json::from_str(&raw).with_context(|| {
                format!(
                    "Existing .mcp.json at {} contains invalid JSON — \
                     refusing to overwrite. Fix or remove it, then retry.",
                    mcp_path.display()
                )
            })?;
            match parsed {
                serde_json::Value::Object(map) => map,
                _ => anyhow::bail!(
                    "Existing .mcp.json at {} is not a JSON object — \
                     refusing to overwrite. Fix or remove it, then retry.",
                    mcp_path.display()
                ),
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => serde_json::Map::new(),
        Err(e) => return Err(anyhow::Error::from(e).context("Failed to read existing .mcp.json")),
    };

    let mut dest_map = match obj.remove("mcpServers") {
        Some(serde_json::Value::Object(map)) => map,
        Some(_) => anyhow::bail!(
            "Existing .mcp.json has a non-object mcpServers value — \
             refusing to overwrite. Fix or remove it, then retry."
        ),
        None => serde_json::Map::new(),
    };

    let mut warnings = Vec::new();
    for (key, value) in src_servers {
        if dest_map.contains_key(key) {
            warnings.push(format!(
                "Warning: overwriting existing mcpServers entry \"{}\" with chainlink default",
                key
            ));
        }
        dest_map.insert(key.clone(), value.clone());
    }

    obj.insert("mcpServers".into(), serde_json::Value::Object(dest_map));

    let mut output = serde_json::to_string_pretty(&serde_json::Value::Object(obj))
        .context("Failed to serialize .mcp.json")?;
    output.push('\n');
    fs::write(mcp_path, output).context("Failed to write .mcp.json")?;
    Ok(warnings)
}

pub fn run(path: &Path, force: bool, db_only: bool) -> Result<()> {
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

    // --db-only: stop after creating the database
    if db_only {
        return Ok(());
    }

    // Write hook config (create or update)
    let config_path = chainlink_dir.join("hook-config.json");
    if !config_path.exists() || force {
        fs::write(&config_path, HOOK_CONFIG_JSON).context("Failed to write hook-config.json")?;
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

    // Create rules.local/ directory for machine-local rule overrides (gitignored)
    let rules_local_dir = chainlink_dir.join("rules.local");
    if !rules_local_dir.exists() {
        fs::create_dir_all(&rules_local_dir)
            .context("Failed to create .chainlink/rules.local directory")?;
    }

    // Write .chainlink/.gitignore for machine-local files
    let inner_gitignore = chainlink_dir.join(".gitignore");
    if !inner_gitignore.exists() || force {
        fs::write(
            &inner_gitignore,
            "# Machine-local overrides (not committed)\nrules.local/\nhook-config.local.json\n.cache/\nagent.json\n.locks-cache/\n",
        )
        .context("Failed to write .chainlink/.gitignore")?;
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

        fs::write(hooks_dir.join("pre-web-check.py"), PRE_WEB_CHECK_PY)
            .context("Failed to write pre-web-check.py")?;

        fs::write(hooks_dir.join("work-check.py"), WORK_CHECK_PY)
            .context("Failed to write work-check.py")?;

        fs::write(hooks_dir.join("chainlink_config.py"), CHAINLINK_CONFIG_PY)
            .context("Failed to write chainlink_config.py")?;

        // Create MCP server directory and write safe-fetch server
        let mcp_dir = claude_dir.join("mcp");
        fs::create_dir_all(&mcp_dir).context("Failed to create .claude/mcp directory")?;
        fs::write(mcp_dir.join("safe-fetch-server.py"), SAFE_FETCH_SERVER_PY)
            .context("Failed to write safe-fetch-server.py")?;

        // Merge chainlink's MCP server entry into .mcp.json (preserving existing MCPs)
        let warnings =
            write_mcp_json_merged(&path.join(".mcp.json")).context("Failed to write .mcp.json")?;
        for warning in warnings {
            println!("{}", warning);
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_run_fresh_init() {
        let dir = tempdir().unwrap();
        let result = run(dir.path(), false, false);
        assert!(result.is_ok());

        // Verify directories created
        assert!(dir.path().join(".chainlink").exists());
        assert!(dir.path().join(".chainlink/rules").exists());
        assert!(dir.path().join(".chainlink/issues.db").exists());
        assert!(dir.path().join(".claude").exists());
        assert!(dir.path().join(".claude/hooks").exists());
        assert!(dir.path().join(".claude/mcp").exists());
        assert!(dir.path().join(".chainlink/hook-config.json").exists());
    }

    #[test]
    fn test_run_creates_hook_files() {
        let dir = tempdir().unwrap();
        run(dir.path(), false, false).unwrap();

        // Verify hook files
        assert!(dir.path().join(".claude/settings.json").exists());
        assert!(dir.path().join(".claude/hooks/prompt-guard.py").exists());
        assert!(dir.path().join(".claude/hooks/post-edit-check.py").exists());
        assert!(dir.path().join(".claude/hooks/session-start.py").exists());
        assert!(dir.path().join(".claude/hooks/pre-web-check.py").exists());
        assert!(dir.path().join(".claude/hooks/work-check.py").exists());
        assert!(dir
            .path()
            .join(".claude/hooks/chainlink_config.py")
            .exists());
        assert!(dir.path().join(".claude/mcp/safe-fetch-server.py").exists());
        assert!(dir.path().join(".mcp.json").exists());
    }

    #[test]
    fn test_run_creates_rule_files() {
        let dir = tempdir().unwrap();
        run(dir.path(), false, false).unwrap();

        let rules_dir = dir.path().join(".chainlink/rules");
        assert!(rules_dir.join("global.md").exists());
        assert!(rules_dir.join("project.md").exists());
        assert!(rules_dir.join("rust.md").exists());
        assert!(rules_dir.join("python.md").exists());
        assert!(rules_dir.join("javascript.md").exists());
        assert!(rules_dir.join("typescript.md").exists());
        assert!(rules_dir.join("tracking-strict.md").exists());
        assert!(rules_dir.join("tracking-normal.md").exists());
        assert!(rules_dir.join("tracking-relaxed.md").exists());
    }

    #[test]
    fn test_run_already_initialized_no_force() {
        let dir = tempdir().unwrap();

        // First init
        run(dir.path(), false, false).unwrap();

        // Second init without force - should succeed but not recreate
        let result = run(dir.path(), false, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_force_update() {
        let dir = tempdir().unwrap();

        // First init
        run(dir.path(), false, false).unwrap();

        // Modify a hook file
        let hook_path = dir.path().join(".claude/hooks/prompt-guard.py");
        fs::write(&hook_path, "# modified").unwrap();

        // Force update
        run(dir.path(), true, false).unwrap();

        // Verify file was restored
        let content = fs::read_to_string(&hook_path).unwrap();
        assert_ne!(content, "# modified");
        assert!(content.contains("python") || content.contains("def") || content.len() > 20);
    }

    /// Keys that the embedded MCP_JSON is expected to manage.
    fn embedded_mcp_keys() -> Vec<String> {
        let embedded: serde_json::Value = serde_json::from_str(MCP_JSON).unwrap();
        embedded["mcpServers"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }

    #[test]
    fn test_force_init_preserves_existing_mcp_servers() {
        let dir = tempdir().unwrap();
        run(dir.path(), false, false).unwrap();

        // Add a custom MCP server entry alongside the embedded ones
        let mcp_path = dir.path().join(".mcp.json");
        let mut content: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&mcp_path).unwrap()).unwrap();
        content["mcpServers"]["my-custom-server"] = serde_json::json!({
            "command": "node",
            "args": ["my-server.js"]
        });
        fs::write(&mcp_path, serde_json::to_string_pretty(&content).unwrap()).unwrap();

        // Force update
        run(dir.path(), true, false).unwrap();

        // Verify all embedded keys and the custom key are present
        let result: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&mcp_path).unwrap()).unwrap();
        let servers = result["mcpServers"].as_object().unwrap();

        for key in embedded_mcp_keys() {
            assert!(
                servers.contains_key(&key),
                "embedded key \"{}\" should exist",
                key
            );
        }
        assert!(
            servers.contains_key("my-custom-server"),
            "custom server should be preserved"
        );
        assert_eq!(
            servers["my-custom-server"]["command"].as_str().unwrap(),
            "node"
        );
    }

    #[test]
    fn test_force_init_returns_warnings_for_overwritten_keys() {
        let dir = tempdir().unwrap();
        run(dir.path(), false, false).unwrap();

        // The first init created .mcp.json with the embedded keys.
        // A second force init should warn about overwriting each one.
        let mcp_path = dir.path().join(".mcp.json");
        let warnings = write_mcp_json_merged(&mcp_path).unwrap();

        let expected_keys = embedded_mcp_keys();
        assert_eq!(
            warnings.len(),
            expected_keys.len(),
            "should warn once per embedded key"
        );
        for key in &expected_keys {
            assert!(
                warnings.iter().any(|w| w.contains(key)),
                "should warn about overwriting \"{}\"",
                key
            );
        }
    }

    #[test]
    fn test_write_mcp_json_merged_creates_fresh_file() {
        let dir = tempdir().unwrap();
        let mcp_path = dir.path().join(".mcp.json");

        // No pre-existing file
        assert!(!mcp_path.exists());

        let warnings = write_mcp_json_merged(&mcp_path).unwrap();
        assert!(
            warnings.is_empty(),
            "fresh creation should produce no warnings"
        );

        let content: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&mcp_path).unwrap()).unwrap();
        let servers = content["mcpServers"].as_object().unwrap();

        // Should contain exactly the embedded keys
        let expected_keys = embedded_mcp_keys();
        assert_eq!(servers.len(), expected_keys.len());
        for key in &expected_keys {
            assert!(
                servers.contains_key(key),
                "fresh file should contain \"{}\"",
                key
            );
        }
    }

    #[test]
    fn test_force_init_fails_on_malformed_mcp_json() {
        let dir = tempdir().unwrap();
        run(dir.path(), false, false).unwrap();

        // Write invalid JSON to .mcp.json
        let mcp_path = dir.path().join(".mcp.json");
        fs::write(&mcp_path, "not json {{{").unwrap();

        // Force init should fail, not silently overwrite
        let result = run(dir.path(), true, false);
        assert!(result.is_err());
        let err = format!("{:#}", result.unwrap_err());
        assert!(
            err.contains("invalid JSON"),
            "Error should mention invalid JSON, got: {}",
            err
        );

        // Original (broken) content should be untouched
        let content = fs::read_to_string(&mcp_path).unwrap();
        assert_eq!(content, "not json {{{");
    }

    #[test]
    fn test_force_init_fails_on_non_object_mcp_json() {
        let dir = tempdir().unwrap();
        run(dir.path(), false, false).unwrap();

        // Write a JSON array to .mcp.json
        let mcp_path = dir.path().join(".mcp.json");
        fs::write(&mcp_path, "[1, 2, 3]").unwrap();

        // Force init should fail, not silently overwrite
        let result = run(dir.path(), true, false);
        assert!(result.is_err());
        let err = format!("{:#}", result.unwrap_err());
        assert!(
            err.contains("not a JSON object"),
            "Error should mention not a JSON object, got: {}",
            err
        );

        // Original content should be untouched
        let content = fs::read_to_string(&mcp_path).unwrap();
        assert_eq!(content, "[1, 2, 3]");
    }

    #[test]
    fn test_force_init_handles_empty_mcp_json_file() {
        let dir = tempdir().unwrap();
        run(dir.path(), false, false).unwrap();

        // Write empty file
        let mcp_path = dir.path().join(".mcp.json");
        fs::write(&mcp_path, "").unwrap();

        // Should fail — empty file is not valid JSON
        let result = run(dir.path(), true, false);
        assert!(result.is_err());
        let err = format!("{:#}", result.unwrap_err());
        assert!(
            err.contains("invalid JSON"),
            "Error should mention invalid JSON, got: {}",
            err
        );
    }

    #[test]
    fn test_force_init_fails_on_non_object_mcp_servers_value() {
        let dir = tempdir().unwrap();
        run(dir.path(), false, false).unwrap();

        // Write valid JSON where mcpServers is a string instead of object
        let mcp_path = dir.path().join(".mcp.json");
        fs::write(&mcp_path, r#"{"mcpServers": "banana"}"#).unwrap();

        // Should fail, not silently replace
        let result = run(dir.path(), true, false);
        assert!(result.is_err());
        let err = format!("{:#}", result.unwrap_err());
        assert!(
            err.contains("non-object mcpServers"),
            "Error should mention non-object mcpServers, got: {}",
            err
        );

        // Original content should be untouched
        let content = fs::read_to_string(&mcp_path).unwrap();
        assert_eq!(content, r#"{"mcpServers": "banana"}"#);
    }

    #[test]
    fn test_init_merges_into_mcp_json_without_mcp_servers_key() {
        let dir = tempdir().unwrap();
        run(dir.path(), false, false).unwrap();

        // Write a valid object with no mcpServers key
        let mcp_path = dir.path().join(".mcp.json");
        fs::write(&mcp_path, r#"{"someOtherKey": true}"#).unwrap();

        // Force init should add mcpServers, preserving the other key
        run(dir.path(), true, false).unwrap();

        let content = fs::read_to_string(&mcp_path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed["someOtherKey"], true);
        assert!(parsed["mcpServers"]["chainlink-safe-fetch"].is_object());
    }

    #[test]
    fn test_run_partial_init_chainlink_only() {
        let dir = tempdir().unwrap();

        // Create only .chainlink directory
        fs::create_dir_all(dir.path().join(".chainlink")).unwrap();

        let result = run(dir.path(), false, false);
        assert!(result.is_ok());

        // .claude should now exist
        assert!(dir.path().join(".claude").exists());
    }

    #[test]
    fn test_run_partial_init_claude_only() {
        let dir = tempdir().unwrap();

        // Create only .claude directory
        fs::create_dir_all(dir.path().join(".claude")).unwrap();

        let result = run(dir.path(), false, false);
        assert!(result.is_ok());

        // .chainlink should now exist
        assert!(dir.path().join(".chainlink").exists());
    }

    #[test]
    fn test_run_database_usable() {
        let dir = tempdir().unwrap();
        run(dir.path(), false, false).unwrap();

        // Open the created database and verify it works
        let db_path = dir.path().join(".chainlink/issues.db");
        let db = Database::open(&db_path).unwrap();

        // Should be able to create an issue
        let id = db.create_issue("Test issue", None, "medium").unwrap();
        assert!(id > 0);
    }

    #[test]
    fn test_run_rule_files_not_empty() {
        let dir = tempdir().unwrap();
        run(dir.path(), false, false).unwrap();

        let rules_dir = dir.path().join(".chainlink/rules");

        // Verify rule files have content
        let global = fs::read_to_string(rules_dir.join("global.md")).unwrap();
        assert!(!global.is_empty());

        let rust = fs::read_to_string(rules_dir.join("rust.md")).unwrap();
        assert!(!rust.is_empty());
    }

    #[test]
    fn test_run_force_updates_rules() {
        let dir = tempdir().unwrap();
        run(dir.path(), false, false).unwrap();

        // Modify a rule file
        let rule_path = dir.path().join(".chainlink/rules/global.md");
        fs::write(&rule_path, "# modified rule").unwrap();

        // Force update
        run(dir.path(), true, false).unwrap();

        // Verify file was restored
        let content = fs::read_to_string(&rule_path).unwrap();
        assert_ne!(content, "# modified rule");
    }

    #[test]
    fn test_run_idempotent_with_force() {
        let dir = tempdir().unwrap();

        // Multiple force runs should all succeed
        for _ in 0..3 {
            let result = run(dir.path(), true, false);
            assert!(result.is_ok());
        }

        // All files should still exist
        assert!(dir.path().join(".chainlink/issues.db").exists());
        assert!(dir.path().join(".claude/settings.json").exists());
    }

    #[test]
    fn test_embedded_constants_not_empty() {
        // Verify all embedded constants have content
        assert!(!SETTINGS_JSON.is_empty());
        assert!(!PROMPT_GUARD_PY.is_empty());
        assert!(!POST_EDIT_CHECK_PY.is_empty());
        assert!(!SESSION_START_PY.is_empty());
        assert!(!PRE_WEB_CHECK_PY.is_empty());
        assert!(!WORK_CHECK_PY.is_empty());
        assert!(!SAFE_FETCH_SERVER_PY.is_empty());
        assert!(!MCP_JSON.is_empty());
        assert!(!RULE_SANITIZE_PATTERNS.is_empty());
        assert!(!HOOK_CONFIG_JSON.is_empty());
        assert!(!RULE_TRACKING_STRICT.is_empty());
        assert!(!RULE_TRACKING_NORMAL.is_empty());
        assert!(!RULE_TRACKING_RELAXED.is_empty());
        assert!(!RULE_GLOBAL.is_empty());
        assert!(!RULE_RUST.is_empty());
    }

    #[test]
    fn test_rule_files_count() {
        // Verify we have the expected number of rule files
        assert!(RULE_FILES.len() >= 20);

        // All should have content
        for (name, content) in RULE_FILES {
            assert!(!name.is_empty(), "Rule file name should not be empty");
            assert!(
                !content.is_empty(),
                "Rule file {} should not be empty",
                name
            );
        }
    }

    #[test]
    fn test_run_db_only_creates_database_only() {
        let dir = tempdir().unwrap();
        let result = run(dir.path(), false, true);
        assert!(result.is_ok());

        // Database and .chainlink should exist
        assert!(dir.path().join(".chainlink").exists());
        assert!(dir.path().join(".chainlink/issues.db").exists());

        // Nothing else should be created
        assert!(!dir.path().join(".chainlink/rules").exists());
        assert!(!dir.path().join(".chainlink/rules.local").exists());
        assert!(!dir.path().join(".chainlink/hook-config.json").exists());
        assert!(!dir.path().join(".claude").exists());
        assert!(!dir.path().join(".mcp.json").exists());
    }

    #[test]
    fn test_run_db_only_is_idempotent() {
        let dir = tempdir().unwrap();
        run(dir.path(), false, true).unwrap();
        // Second call should succeed without error
        let result = run(dir.path(), false, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_run_db_only_database_usable() {
        let dir = tempdir().unwrap();
        run(dir.path(), false, true).unwrap();

        let db_path = dir.path().join(".chainlink/issues.db");
        let db = Database::open(&db_path).unwrap();
        let id = db.create_issue("Test issue", None, "medium").unwrap();
        assert!(id > 0);
    }
}
