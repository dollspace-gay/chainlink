//! Code clone detection via cpitd (Copy Paste Is The Devil).
//!
//! Shells out to the `cpitd` Python tool, parses its JSON output,
//! and creates chainlink issues for detected code clones.

use std::collections::HashMap;
use std::process::Command;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use crate::db::Database;

// ---------------------------------------------------------------------------
// cpitd JSON output types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct CpitdOutput {
    clone_reports: Vec<CpitdCloneReport>,
    total_pairs: usize,
}

#[derive(Debug, Deserialize)]
struct CpitdCloneReport {
    file_a: String,
    file_b: String,
    total_cloned_lines: usize,
    groups: Vec<CpitdCloneGroup>,
}

#[derive(Debug, Deserialize)]
struct CpitdCloneGroup {
    lines_a: Vec<usize>,
    lines_b: Vec<usize>,
    line_count: usize,
    token_count: usize,
}

// ---------------------------------------------------------------------------
// Installation detection
// ---------------------------------------------------------------------------

fn find_cpitd() -> bool {
    Command::new("cpitd")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn suggest_install() -> Result<()> {
    bail!(
        "cpitd is not installed or not found in PATH.\n\n\
         cpitd (Copy Paste Is The Devil) is a language-agnostic code clone\n\
         detector that integrates with chainlink to track duplicated code\n\
         as issues.\n\n\
         Install it with:\n\n\
         \x20 pip install cpitd\n\n\
         Or visit: https://github.com/scythia-marrow/cpitd"
    )
}

// ---------------------------------------------------------------------------
// Running cpitd
// ---------------------------------------------------------------------------

fn run_cpitd(paths: &[String], min_tokens: u32, ignore_patterns: &[String]) -> Result<CpitdOutput> {
    let mut cmd = Command::new("cpitd");

    if paths.is_empty() {
        cmd.arg(".");
    } else {
        for p in paths {
            cmd.arg(p);
        }
    }

    cmd.arg("--format").arg("json");
    cmd.arg("--min-tokens").arg(min_tokens.to_string());

    for pattern in ignore_patterns {
        cmd.arg("--ignore").arg(pattern);
    }

    let output = cmd.output().context("Failed to execute cpitd")?;

    let stdout = String::from_utf8(output.stdout).context("cpitd output is not valid UTF-8")?;

    if stdout.trim().is_empty() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.trim().is_empty() {
            // cpitd exited 0 with no output means no clones in some edge cases
            return Ok(CpitdOutput {
                clone_reports: vec![],
                total_pairs: 0,
            });
        }
        bail!("cpitd produced no output. stderr: {}", stderr.trim());
    }

    serde_json::from_str(&stdout).context("Failed to parse cpitd JSON output")
}

// ---------------------------------------------------------------------------
// Deduplication
// ---------------------------------------------------------------------------

fn dedup_marker(file_a: &str, file_b: &str) -> String {
    let (a, b) = if file_a <= file_b {
        (file_a, file_b)
    } else {
        (file_b, file_a)
    };
    format!("<!-- cpitd:file_a={}:file_b={} -->", a, b)
}

fn find_existing_clone_issue(db: &Database, file_a: &str, file_b: &str) -> Result<Option<i64>> {
    let marker = dedup_marker(file_a, file_b);
    let issues = db.list_issues(Some("open"), &["cpitd".to_string()], None)?;
    for issue in issues {
        if let Some(ref desc) = issue.description {
            if desc.contains(&marker) {
                return Ok(Some(issue.id));
            }
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// Issue creation
// ---------------------------------------------------------------------------

fn shorten_path(path: &str) -> &str {
    std::path::Path::new(path)
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or(path)
}

fn format_clone_description(report: &CpitdCloneReport) -> String {
    let marker = dedup_marker(&report.file_a, &report.file_b);

    let mut desc = format!(
        "{}\n\n\
         Detected code clones between:\n\
         - `{}`\n\
         - `{}`\n\n\
         Total cloned lines: {}\n\n\
         Clone groups:\n",
        marker, report.file_a, report.file_b, report.total_cloned_lines,
    );

    for (i, group) in report.groups.iter().enumerate() {
        desc.push_str(&format!(
            "{}. Lines {}-{} <-> Lines {}-{} ({} lines, {} tokens)\n",
            i + 1,
            group.lines_a[0],
            group.lines_a[1],
            group.lines_b[0],
            group.lines_b[1],
            group.line_count,
            group.token_count,
        ));
    }

    desc.push_str("\nConsider extracting shared logic into a common function or module.");
    desc
}

fn create_clone_issue(db: &Database, report: &CpitdCloneReport, quiet: bool) -> Result<i64> {
    let title = format!(
        "Code clone: {} <-> {} ({} lines)",
        shorten_path(&report.file_a),
        shorten_path(&report.file_b),
        report.total_cloned_lines,
    );

    let description = format_clone_description(report);
    let id = db.create_issue(&title, Some(&description), "low")?;
    db.add_label(id, "cpitd")?;
    db.add_label(id, "refactor")?;

    if !quiet {
        println!("  Created issue #{}: {}", id, title);
    }

    Ok(id)
}

fn relate_clone_issues(db: &Database, created: &[(i64, String, String)]) -> Result<()> {
    let mut file_to_issues: HashMap<&str, Vec<i64>> = HashMap::new();
    for (id, file_a, file_b) in created {
        file_to_issues.entry(file_a).or_default().push(*id);
        file_to_issues.entry(file_b).or_default().push(*id);
    }
    for ids in file_to_issues.values() {
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                // Ignore errors (e.g. relation already exists)
                let _ = db.add_relation(ids[i], ids[j]);
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Public commands
// ---------------------------------------------------------------------------

pub fn scan(
    db: &Database,
    paths: &[String],
    min_tokens: u32,
    ignore_patterns: &[String],
    dry_run: bool,
    quiet: bool,
) -> Result<()> {
    if !find_cpitd() {
        return suggest_install();
    }

    if !quiet {
        println!("Running cpitd clone detection...");
    }

    let output = run_cpitd(paths, min_tokens, ignore_patterns)?;

    if output.clone_reports.is_empty() {
        if !quiet {
            println!("No code clones detected.");
        }
        return Ok(());
    }

    if !quiet {
        println!("Found {} clone pair(s).\n", output.total_pairs);
    }

    if dry_run {
        for report in &output.clone_reports {
            println!(
                "  Would create: {} <-> {} ({} lines, {} group(s))",
                report.file_a,
                report.file_b,
                report.total_cloned_lines,
                report.groups.len(),
            );
        }
        return Ok(());
    }

    let mut created_count = 0usize;
    let mut updated_count = 0usize;
    let mut created_ids: Vec<(i64, String, String)> = Vec::new();

    for report in &output.clone_reports {
        match find_existing_clone_issue(db, &report.file_a, &report.file_b)? {
            Some(existing_id) => {
                let comment = format!(
                    "[cpitd rescan] {} total cloned lines, {} group(s)",
                    report.total_cloned_lines,
                    report.groups.len(),
                );
                db.add_comment(existing_id, &comment, "note")?;
                updated_count += 1;
                if !quiet {
                    println!("  Updated issue #{} (clone still present)", existing_id);
                }
            }
            None => {
                let id = create_clone_issue(db, report, quiet)?;
                created_ids.push((id, report.file_a.clone(), report.file_b.clone()));
                created_count += 1;
            }
        }
    }

    if created_ids.len() > 1 {
        relate_clone_issues(db, &created_ids)?;
    }

    if !quiet {
        println!(
            "\ncpitd scan complete: {} created, {} updated",
            created_count, updated_count,
        );
    }

    Ok(())
}

pub fn status(db: &Database) -> Result<()> {
    let issues = db.list_issues(Some("open"), &["cpitd".to_string()], None)?;

    if issues.is_empty() {
        println!("No open cpitd clone issues.");
    } else {
        println!("{} open clone issue(s):\n", issues.len());
        for issue in &issues {
            println!("  #{:<4} {}", issue.id, issue.title);
        }
    }

    Ok(())
}

pub fn clear(db: &Database) -> Result<()> {
    let issues = db.list_issues(Some("open"), &["cpitd".to_string()], None)?;

    if issues.is_empty() {
        println!("No open cpitd clone issues to close.");
        return Ok(());
    }

    let count = issues.len();
    for issue in &issues {
        db.close_issue(issue.id)?;
    }

    println!("Closed {} cpitd clone issue(s).", count);
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cpitd_json() {
        let json = r#"{
            "clone_reports": [
                {
                    "file_a": "src/foo.rs",
                    "file_b": "src/bar.rs",
                    "total_cloned_lines": 15,
                    "groups": [
                        {
                            "lines_a": [10, 24],
                            "lines_b": [30, 44],
                            "line_count": 15,
                            "token_count": 120
                        }
                    ]
                }
            ],
            "total_pairs": 1
        }"#;

        let output: CpitdOutput = serde_json::from_str(json).unwrap();
        assert_eq!(output.total_pairs, 1);
        assert_eq!(output.clone_reports.len(), 1);
        assert_eq!(output.clone_reports[0].file_a, "src/foo.rs");
        assert_eq!(output.clone_reports[0].file_b, "src/bar.rs");
        assert_eq!(output.clone_reports[0].total_cloned_lines, 15);
        assert_eq!(output.clone_reports[0].groups.len(), 1);
        assert_eq!(output.clone_reports[0].groups[0].lines_a, vec![10, 24]);
        assert_eq!(output.clone_reports[0].groups[0].line_count, 15);
        assert_eq!(output.clone_reports[0].groups[0].token_count, 120);
    }

    #[test]
    fn test_parse_cpitd_empty() {
        let json = r#"{"clone_reports": [], "total_pairs": 0}"#;
        let output: CpitdOutput = serde_json::from_str(json).unwrap();
        assert_eq!(output.total_pairs, 0);
        assert!(output.clone_reports.is_empty());
    }

    #[test]
    fn test_dedup_marker_commutative() {
        let m1 = dedup_marker("src/a.rs", "src/b.rs");
        let m2 = dedup_marker("src/b.rs", "src/a.rs");
        assert_eq!(m1, m2);
    }

    #[test]
    fn test_dedup_marker_contains_paths() {
        let marker = dedup_marker("foo.py", "bar.py");
        assert!(marker.contains("bar.py"));
        assert!(marker.contains("foo.py"));
        assert!(marker.starts_with("<!-- cpitd:"));
        assert!(marker.ends_with(" -->"));
    }

    #[test]
    fn test_shorten_path_basic() {
        assert_eq!(shorten_path("src/commands/foo.rs"), "foo.rs");
        assert_eq!(shorten_path("bar.py"), "bar.py");
        assert_eq!(shorten_path("a/b/c/d.txt"), "d.txt");
    }

    #[test]
    fn test_format_clone_description() {
        let report = CpitdCloneReport {
            file_a: "src/foo.rs".to_string(),
            file_b: "src/bar.rs".to_string(),
            total_cloned_lines: 10,
            groups: vec![CpitdCloneGroup {
                lines_a: vec![1, 10],
                lines_b: vec![20, 29],
                line_count: 10,
                token_count: 80,
            }],
        };
        let desc = format_clone_description(&report);
        assert!(desc.contains("<!-- cpitd:"));
        assert!(desc.contains("src/bar.rs"));
        assert!(desc.contains("src/foo.rs"));
        assert!(desc.contains("10"));
        assert!(desc.contains("80 tokens"));
    }
}
