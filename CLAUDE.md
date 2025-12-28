# Chainlink Issue Tracker

A simple, lean issue tracker for AI-assisted development. Use this to track tasks across sessions.

## Quick Start

```bash
# Initialize in any project
chainlink init

# Start a session when you begin work
chainlink session start

# Create issues
chainlink create "Fix login bug" -p high
chainlink create "Add dark mode" -d "Support light/dark theme toggle"

# Set what you're working on
chainlink session work 1
```

## Commands Reference

### Issue Management

```bash
chainlink create <title>              # Create issue (returns ID)
chainlink create <title> -p high      # With priority (low/medium/high/critical)
chainlink create <title> -d "desc"    # With description

chainlink list                        # List open issues
chainlink list -s all                 # List all issues
chainlink list -s closed              # List closed issues
chainlink list -l bug                 # Filter by label
chainlink list -p high                # Filter by priority

chainlink show <id>                   # Show issue details
chainlink update <id> --title "New"   # Update title
chainlink update <id> -d "New desc"   # Update description
chainlink update <id> -p critical     # Update priority

chainlink close <id>                  # Close issue
chainlink reopen <id>                 # Reopen closed issue
chainlink delete <id>                 # Delete (with confirmation)
chainlink delete <id> -f              # Delete without confirmation
```

### Comments & Labels

```bash
chainlink comment <id> "Found the bug in auth.go"
chainlink label <id> bug
chainlink label <id> urgent
chainlink unlabel <id> urgent
```

### Dependencies

```bash
chainlink block <id> <blocker_id>     # Mark issue blocked by another
chainlink unblock <id> <blocker_id>   # Remove blocking relationship
chainlink blocked                     # List all blocked issues
chainlink ready                       # List issues ready to work on
```

### Session Management (Context Preservation)

Sessions preserve context across Claude restarts. Always use them.

```bash
chainlink session start               # Start session, shows previous handoff notes
chainlink session work <id>           # Set the issue you're working on
chainlink session status              # Show current session info
chainlink session end                 # End session
chainlink session end --notes "..."   # End with handoff notes for next session
```

**Workflow:**
1. `session start` - See what the previous session was doing
2. `session work <id>` - Mark what you're working on
3. Do your work, add comments as you go
4. `session end --notes "context"` - Save notes for next session

### Daemon (Optional)

The daemon auto-flushes session state every 30 seconds.

```bash
chainlink daemon start                # Start background daemon
chainlink daemon status               # Check if running
chainlink daemon stop                 # Stop daemon
```

## Best Practices

1. **Always start a session** when beginning work
2. **Use `session work`** to mark current focus
3. **Add comments** as you discover things
4. **End with handoff notes** before context gets compressed
5. **Use `ready`** to find unblocked work
6. **Use dependencies** to track blocking relationships

## Storage

All data is in `.chainlink/issues.db` (SQLite). No git hooks, no auto-push, no sync complexity.

## Example Session

```bash
$ chainlink session start
Previous session ended: 2024-01-15 09:00
Handoff notes:
  Working on auth bug. Found issue in token refresh - line 145 of auth.go.

Session #5 started.

$ chainlink session work 1
Now working on: #1 Fix login bug

$ chainlink comment 1 "Fixed the token refresh issue"
Added comment to issue #1

$ chainlink close 1
Closed issue #1

$ chainlink ready
Ready issues (no blockers):
  #2    medium   Add dark mode

$ chainlink session end --notes "Closed auth bug. Dark mode is next."
Session #5 ended.
Handoff notes saved.
```

---

## Rust Best Practices

Follow these guidelines when writing Rust code in this project.

### Code Style

- Use `rustfmt` for formatting (run `cargo fmt` before committing)
- Use `clippy` for linting (run `cargo clippy -- -D warnings`)
- Prefer `?` operator over `.unwrap()` for error handling
- Use `anyhow::Result` for application errors, `thiserror` for library errors
- Avoid `.clone()` unless necessary - prefer references
- Use `&str` for function parameters, `String` for owned data

### Error Handling

```rust
// GOOD: Propagate errors with context
fn read_config(path: &Path) -> Result<Config> {
    let content = fs::read_to_string(path)
        .context("Failed to read config file")?;
    serde_json::from_str(&content)
        .context("Failed to parse config")
}

// BAD: Panic on error
fn read_config(path: &Path) -> Config {
    let content = fs::read_to_string(path).unwrap();  // Don't do this
    serde_json::from_str(&content).unwrap()
}
```

### Memory Safety

- Never use `unsafe` without explicit justification and review
- Prefer `Vec` over raw pointers
- Use `Arc<Mutex<T>>` for shared mutable state across threads
- Avoid `static mut` - use `lazy_static` or `once_cell` instead

---

## Security Requirements

All code must follow these security practices. Non-negotiable.

### Input Validation

- **Validate ALL user input** before processing
- **Sanitize paths** - prevent path traversal attacks
- **Limit input sizes** - prevent DoS via memory exhaustion

```rust
// GOOD: Validate and sanitize
fn process_filename(name: &str) -> Result<PathBuf> {
    // Reject path traversal attempts
    if name.contains("..") || name.contains('/') || name.contains('\\') {
        bail!("Invalid filename: path traversal detected");
    }
    // Limit length
    if name.len() > 255 {
        bail!("Filename too long");
    }
    Ok(PathBuf::from(name))
}

// BAD: Trust user input
fn process_filename(name: &str) -> PathBuf {
    PathBuf::from(name)  // Allows ../../../etc/passwd
}
```

### SQL Injection Prevention

- **ALWAYS use parameterized queries** - never string concatenation
- Use `rusqlite::params![]` macro for parameters

```rust
// GOOD: Parameterized query
conn.execute(
    "INSERT INTO users (name, email) VALUES (?1, ?2)",
    params![name, email],
)?;

// BAD: String concatenation (SQL injection vulnerability)
conn.execute(
    &format!("INSERT INTO users (name) VALUES ('{}')", name),
    [],
)?;
```

### Command Injection Prevention

- **Never pass user input directly to shell commands**
- Use `Command::new()` with explicit args, not shell strings
- Validate/sanitize all inputs used in commands

```rust
// GOOD: Explicit command with args
Command::new("git")
    .args(["clone", "--depth", "1", url])
    .status()?;

// BAD: Shell string (command injection vulnerability)
Command::new("sh")
    .args(["-c", &format!("git clone {}", url)])  // url could be "; rm -rf /"
    .status()?;
```

### Secrets Management

- **Never hardcode secrets** (API keys, passwords, tokens)
- Use environment variables or secure config files
- Never log secrets - mask them in error messages
- Add `.env` and credentials files to `.gitignore`

```rust
// GOOD: Read from environment
let api_key = std::env::var("API_KEY")
    .context("API_KEY environment variable not set")?;

// BAD: Hardcoded secret
let api_key = "sk-1234567890abcdef";  // Never do this
```

### Cryptography

- Use well-established crates: `ring`, `rustls`, `argon2`
- Never implement your own crypto
- Use `argon2` or `bcrypt` for password hashing (never SHA256/MD5)
- Use secure random number generation (`rand::rngs::OsRng`)

### OWASP Top 10 Checklist

Before submitting code, verify:
- [ ] No SQL injection vulnerabilities (parameterized queries only)
- [ ] No command injection (no shell strings with user input)
- [ ] No path traversal (validate file paths)
- [ ] No hardcoded secrets
- [ ] No sensitive data in logs
- [ ] Input validation on all external data
- [ ] Proper error handling (no stack traces to users)
- [ ] Dependencies are up-to-date (`cargo audit`)

---

## Unit Testing

All code should have corresponding tests. Run tests with `cargo test`.

### Test Organization

```
src/
├── lib.rs
├── db.rs
└── commands/
    └── create.rs

tests/                    # Integration tests
└── integration_test.rs
```

### Writing Unit Tests

```rust
// In src/db.rs
pub fn validate_priority(p: &str) -> bool {
    matches!(p, "low" | "medium" | "high" | "critical")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_priority_valid() {
        assert!(validate_priority("low"));
        assert!(validate_priority("medium"));
        assert!(validate_priority("high"));
        assert!(validate_priority("critical"));
    }

    #[test]
    fn test_validate_priority_invalid() {
        assert!(!validate_priority(""));
        assert!(!validate_priority("urgent"));
        assert!(!validate_priority("LOW"));  // Case sensitive
    }

    #[test]
    fn test_validate_priority_malicious() {
        // Security: ensure no injection vectors
        assert!(!validate_priority("'; DROP TABLE issues; --"));
        assert!(!validate_priority("high\0medium"));
    }
}
```

### Testing Database Operations

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn setup_test_db() -> Database {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        Database::open(&db_path).unwrap()
    }

    #[test]
    fn test_create_and_get_issue() {
        let db = setup_test_db();

        let id = db.create_issue("Test issue", None, "medium").unwrap();
        let issue = db.get_issue(id).unwrap().unwrap();

        assert_eq!(issue.title, "Test issue");
        assert_eq!(issue.status, "open");
        assert_eq!(issue.priority, "medium");
    }

    #[test]
    fn test_close_issue() {
        let db = setup_test_db();
        let id = db.create_issue("Test", None, "low").unwrap();

        db.close_issue(id).unwrap();

        let issue = db.get_issue(id).unwrap().unwrap();
        assert_eq!(issue.status, "closed");
        assert!(issue.closed_at.is_some());
    }
}
```

### Test Commands

```bash
cargo test                    # Run all tests
cargo test -- --nocapture     # Show println! output
cargo test test_name          # Run specific test
cargo test -- --test-threads=1  # Run tests sequentially
```

### Test Coverage

Use `cargo-tarpaulin` for coverage reports:

```bash
cargo install cargo-tarpaulin
cargo tarpaulin --out Html
```

### Property-Based Testing

For complex logic, use `proptest`:

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_priority_roundtrip(p in "low|medium|high|critical") {
        assert!(validate_priority(&p));
    }

    #[test]
    fn test_no_crash_on_arbitrary_input(s in ".*") {
        // Should not panic on any input
        let _ = validate_priority(&s);
    }
}
```

### Security Testing

```rust
#[test]
fn test_sql_injection_prevention() {
    let db = setup_test_db();

    // These should not cause SQL injection
    let malicious_inputs = [
        "'; DROP TABLE issues; --",
        "\" OR 1=1 --",
        "test\0null",
        "a".repeat(10000),
    ];

    for input in malicious_inputs {
        // Should either succeed safely or return an error
        // Should NEVER execute injected SQL
        let result = db.create_issue(input, None, "medium");
        // Verify the database is still intact
        assert!(db.list_issues(None, None, None).is_ok());
    }
}
```

### Mocking

Use `mockall` for mocking traits in tests:

```rust
#[cfg(test)]
use mockall::{automock, predicate::*};

#[automock]
trait Storage {
    fn save(&self, data: &str) -> Result<()>;
}

#[test]
fn test_with_mock() {
    let mut mock = MockStorage::new();
    mock.expect_save()
        .with(eq("test"))
        .times(1)
        .returning(|_| Ok(()));

    // Use mock in your test
}
