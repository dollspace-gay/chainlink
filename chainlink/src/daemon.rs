use anyhow::{Context, Result};
use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::db::Database;

const FLUSH_INTERVAL_SECS: u64 = 30;

pub fn start(chainlink_dir: &Path) -> Result<()> {
    let pid_file = chainlink_dir.join("daemon.pid");
    let log_file = chainlink_dir.join("daemon.log");

    // Check if daemon is already running
    if let Some(pid) = read_pid(&pid_file) {
        if is_process_running(pid) {
            println!("Daemon already running (PID {})", pid);
            return Ok(());
        }
        // Stale PID file, remove it
        let _ = fs::remove_file(&pid_file);
    }

    // Get the current executable path
    let exe = std::env::current_exe().context("Failed to get executable path")?;

    // Spawn the daemon process
    let child = Command::new(&exe)
        .arg("daemon")
        .arg("run")
        .arg("--dir")
        .arg(chainlink_dir)
        .stdin(Stdio::null())
        .stdout(fs::File::create(&log_file).context("Failed to create log file")?)
        .stderr(fs::File::create(&log_file).context("Failed to create log file")?)
        .spawn()
        .context("Failed to spawn daemon process")?;

    let pid = child.id();

    // Write PID file
    fs::write(&pid_file, pid.to_string()).context("Failed to write PID file")?;

    println!("Daemon started (PID {})", pid);
    println!("Log file: {}", log_file.display());
    Ok(())
}

pub fn stop(chainlink_dir: &Path) -> Result<()> {
    let pid_file = chainlink_dir.join("daemon.pid");

    let pid = match read_pid(&pid_file) {
        Some(p) => p,
        None => {
            println!("Daemon not running (no PID file)");
            return Ok(());
        }
    };

    if !is_process_running(pid) {
        fs::remove_file(&pid_file).ok();
        println!("Daemon not running (stale PID file removed)");
        return Ok(());
    }

    // Kill the process
    kill_process(pid)?;

    // Remove PID file
    fs::remove_file(&pid_file).ok();

    println!("Daemon stopped (PID {})", pid);
    Ok(())
}

pub fn status(chainlink_dir: &Path) -> Result<()> {
    let pid_file = chainlink_dir.join("daemon.pid");

    match read_pid(&pid_file) {
        Some(pid) => {
            if is_process_running(pid) {
                println!("Daemon running (PID {})", pid);
            } else {
                println!("Daemon not running (stale PID file)");
            }
        }
        None => {
            println!("Daemon not running");
        }
    }
    Ok(())
}

pub fn run_daemon(chainlink_dir: &Path) -> Result<()> {
    // Validate that this is a legitimate chainlink directory
    let db_path = chainlink_dir.join("issues.db");
    if !db_path.exists() {
        anyhow::bail!(
            "Invalid chainlink directory: {} does not contain issues.db",
            chainlink_dir.display()
        );
    }

    let session_file = chainlink_dir.join("session.json");

    println!("Daemon starting...");
    println!("Watching: {}", chainlink_dir.display());
    println!("Flush interval: {} seconds", FLUSH_INTERVAL_SECS);

    // Zombie prevention: Monitor stdin for closure.
    // When the parent process (VS Code) dies, stdin will be closed.
    // This thread detects that and signals the main loop to exit.
    let should_exit = Arc::new(AtomicBool::new(false));
    let should_exit_clone = Arc::clone(&should_exit);

    thread::spawn(move || {
        let mut stdin = std::io::stdin();
        let mut buf = [0u8; 1];
        // This will block until stdin is closed or data is received
        // When the parent dies, read() returns 0 (EOF) or an error
        loop {
            match stdin.read(&mut buf) {
                Ok(0) => {
                    // EOF - parent closed stdin, time to exit
                    eprintln!("Stdin closed, daemon shutting down (zombie prevention)");
                    should_exit_clone.store(true, Ordering::SeqCst);
                    break;
                }
                Err(_) => {
                    // Error reading stdin - parent likely crashed
                    eprintln!("Stdin error, daemon shutting down (zombie prevention)");
                    should_exit_clone.store(true, Ordering::SeqCst);
                    break;
                }
                Ok(_) => {
                    // Data received (unexpected, but continue)
                    continue;
                }
            }
        }
    });

    loop {
        // Check if we should exit (stdin closed)
        if should_exit.load(Ordering::SeqCst) {
            println!("Daemon exiting due to parent termination");
            break;
        }

        thread::sleep(Duration::from_secs(FLUSH_INTERVAL_SECS));

        // Check again after sleep
        if should_exit.load(Ordering::SeqCst) {
            println!("Daemon exiting due to parent termination");
            break;
        }

        // Auto-flush: read current session and write to session.json
        if let Ok(db) = Database::open(&db_path) {
            if let Ok(Some(session)) = db.get_current_session() {
                let session_data = serde_json::json!({
                    "session_id": session.id,
                    "started_at": session.started_at.to_rfc3339(),
                    "active_issue_id": session.active_issue_id,
                });

                if let Ok(json) = serde_json::to_string_pretty(&session_data) {
                    if let Err(e) = fs::write(&session_file, json) {
                        eprintln!("Failed to write session file: {}", e);
                    } else {
                        println!(
                            "Session flushed at {}",
                            chrono::Utc::now().format("%H:%M:%S")
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

fn read_pid(pid_file: &Path) -> Option<u32> {
    let mut file = fs::File::open(pid_file).ok()?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).ok()?;
    contents.trim().parse().ok()
}

#[cfg(windows)]
fn is_process_running(pid: u32) -> bool {
    use std::process::Command;
    Command::new("tasklist")
        .args(["/FI", &format!("PID eq {}", pid), "/NH"])
        .output()
        .map(|output| {
            let stdout = String::from_utf8_lossy(&output.stdout);
            stdout.contains(&pid.to_string())
        })
        .unwrap_or(false)
}

#[cfg(not(windows))]
fn is_process_running(pid: u32) -> bool {
    use std::process::Command;
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(windows)]
fn kill_process(pid: u32) -> Result<()> {
    use std::process::Command;
    Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/F"])
        .status()
        .context("Failed to kill process")?;
    Ok(())
}

#[cfg(not(windows))]
fn kill_process(pid: u32) -> Result<()> {
    use std::process::Command;
    Command::new("kill")
        .arg(pid.to_string())
        .status()
        .context("Failed to kill process")?;
    Ok(())
}
