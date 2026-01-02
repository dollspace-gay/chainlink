use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

pub fn run(chainlink_dir: &Path) -> Result<()> {
    let marker_file = chainlink_dir.join("last_test_run");

    // Create or update the marker file
    fs::write(&marker_file, "").context("Failed to update test marker")?;

    println!("✓ Marked tests as run");
    println!("  Test reminder will reset on next code change.");

    Ok(())
}
