mod commands;
mod daemon;
mod db;
mod identity;
mod lock_check;
mod locks;
mod models;
mod sync;
mod token_usage;
mod utils;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use std::env;
use std::path::PathBuf;

use db::Database;

#[derive(Parser)]
#[command(name = "chainlink")]
#[command(about = "A simple, lean issue tracker CLI")]
#[command(version = option_env!("CHAINLINK_VERSION").unwrap_or(env!("CARGO_PKG_VERSION")))]
struct Cli {
    /// Quiet mode: only output essential data (IDs, counts)
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Output as JSON (supported by list, show, search, session status)
    #[arg(long, global = true)]
    json: bool,

    /// Log level for diagnostic output (error, warn, info, debug, trace)
    #[arg(long, global = true, default_value = "warn", env = "CHAINLINK_LOG")]
    log_level: String,

    /// Log format (text, json)
    #[arg(
        long,
        global = true,
        default_value = "text",
        env = "CHAINLINK_LOG_FORMAT"
    )]
    log_format: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize chainlink in the current directory
    Init {
        /// Force update hooks even if already initialized
        #[arg(short, long)]
        force: bool,
    },

    /// Issue lifecycle commands (create, show, list, close, ...)
    Issue {
        #[command(subcommand)]
        action: IssueCommands,
    },

    /// Time tracking (start, stop, show)
    Timer {
        #[command(subcommand)]
        action: Option<TimerCommands>,
    },

    /// Export issues to JSON or markdown
    Export {
        /// Output file path (defaults to stdout)
        #[arg(short, long)]
        output: Option<String>,
        /// Format (json, markdown)
        #[arg(short, long, default_value = "json")]
        format: String,
    },

    /// Import issues from JSON file
    Import {
        /// Input file path
        input: String,
    },

    /// Archive management
    Archive {
        #[command(subcommand)]
        action: ArchiveCommands,
    },

    /// Milestone management
    Milestone {
        #[command(subcommand)]
        action: MilestoneCommands,
    },

    /// Session management
    Session {
        #[command(subcommand)]
        action: SessionCommands,
    },

    /// Daemon management
    Daemon {
        #[command(subcommand)]
        action: DaemonCommands,
    },

    /// Code clone detection via cpitd
    Cpitd {
        #[command(subcommand)]
        action: CpitdCommands,
    },

    /// Token usage tracking and cost monitoring
    Usage {
        #[command(subcommand)]
        action: UsageCommands,
    },

    /// Agent identity management
    Agent {
        #[command(subcommand)]
        action: AgentCommands,
    },

    /// Lock management for multi-agent coordination
    Locks {
        #[command(subcommand)]
        action: LocksCommands,
    },

    /// Fetch locks and report coordination status
    Sync,

    // ========================================================================
    // Hidden backward-compatible aliases (flat commands)
    // ========================================================================
    /// Create a new issue (shortcut for `issue create`)
    #[command(hide = true)]
    Create {
        /// Issue title
        title: String,
        /// Issue description
        #[arg(short, long)]
        description: Option<String>,
        /// Priority (low, medium, high, critical)
        #[arg(short, long, default_value = "medium")]
        priority: String,
        /// Template (bug, feature, refactor, research)
        #[arg(short, long)]
        template: Option<String>,
        /// Add labels to the issue
        #[arg(short, long)]
        label: Vec<String>,
        /// Set as current session work item
        #[arg(short, long)]
        work: bool,
    },

    /// Quick-create an issue (shortcut for `issue quick`)
    #[command(hide = true)]
    Quick {
        /// Issue title
        title: String,
        /// Issue description
        #[arg(short, long)]
        description: Option<String>,
        /// Priority (low, medium, high, critical)
        #[arg(short, long, default_value = "medium")]
        priority: String,
        /// Template (bug, feature, refactor, research)
        #[arg(short, long)]
        template: Option<String>,
        /// Add labels to the issue
        #[arg(short, long)]
        label: Vec<String>,
    },

    /// Create a subissue (shortcut for `issue subissue`)
    #[command(hide = true)]
    Subissue {
        /// Parent issue ID
        parent: i64,
        /// Subissue title
        title: String,
        /// Subissue description
        #[arg(short, long)]
        description: Option<String>,
        /// Priority (low, medium, high, critical)
        #[arg(short, long, default_value = "medium")]
        priority: String,
        /// Add labels to the subissue
        #[arg(short, long)]
        label: Vec<String>,
        /// Set as current session work item
        #[arg(short, long)]
        work: bool,
    },

    /// List issues (shortcut for `issue list`)
    #[command(hide = true)]
    List {
        /// Filter by status (open, closed, all)
        #[arg(short, long, default_value = "open")]
        status: String,
        /// Filter by label (repeatable, AND logic)
        #[arg(short, long)]
        label: Vec<String>,
        /// Filter by priority
        #[arg(short, long)]
        priority: Option<String>,
    },

    /// Search issues (shortcut for `issue search`)
    #[command(hide = true)]
    Search {
        /// Search query
        query: String,
    },

    /// Show issue details (shortcut for `issue show`)
    #[command(hide = true)]
    Show {
        /// Issue ID
        id: i64,
    },

    /// Update an issue (shortcut for `issue update`)
    #[command(hide = true)]
    Update {
        /// Issue ID
        id: i64,
        /// New title
        #[arg(short, long)]
        title: Option<String>,
        /// New description
        #[arg(short, long)]
        description: Option<String>,
        /// New priority
        #[arg(short, long)]
        priority: Option<String>,
    },

    /// Close an issue (shortcut for `issue close`)
    #[command(hide = true)]
    Close {
        /// Issue ID
        id: i64,
        /// Skip changelog entry
        #[arg(long)]
        no_changelog: bool,
    },

    /// Close all issues matching filters (shortcut for `issue close-all`)
    #[command(hide = true)]
    CloseAll {
        /// Filter by label (repeatable, AND logic)
        #[arg(short, long)]
        label: Vec<String>,
        /// Filter by priority
        #[arg(short, long)]
        priority: Option<String>,
        /// Skip changelog entries
        #[arg(long)]
        no_changelog: bool,
    },

    /// Reopen an issue (shortcut for `issue reopen`)
    #[command(hide = true)]
    Reopen {
        /// Issue ID
        id: i64,
    },

    /// Delete an issue (shortcut for `issue delete`)
    #[command(hide = true)]
    Delete {
        /// Issue ID
        id: i64,
        /// Skip confirmation
        #[arg(short, long)]
        force: bool,
    },

    /// Add a comment (shortcut for `issue comment`)
    #[command(hide = true)]
    Comment {
        /// Issue ID
        id: i64,
        /// Comment text
        text: String,
        /// Comment kind (note, plan, decision, observation, blocker, resolution, result, handoff, human)
        #[arg(long, default_value = "note")]
        kind: String,
    },

    /// Add a label (shortcut for `issue label`)
    #[command(hide = true)]
    Label {
        /// Issue ID
        id: i64,
        /// Label name
        label: String,
    },

    /// Remove a label (shortcut for `issue unlabel`)
    #[command(hide = true)]
    Unlabel {
        /// Issue ID
        id: i64,
        /// Label name
        label: String,
    },

    /// Block an issue (shortcut for `issue block`)
    #[command(hide = true)]
    Block {
        /// Issue ID that is blocked
        id: i64,
        /// Issue ID that is blocking
        blocker: i64,
    },

    /// Unblock an issue (shortcut for `issue unblock`)
    #[command(hide = true)]
    Unblock {
        /// Issue ID that was blocked
        id: i64,
        /// Issue ID that was blocking
        blocker: i64,
    },

    /// List blocked issues (shortcut for `issue blocked`)
    #[command(hide = true)]
    Blocked,

    /// List ready issues (shortcut for `issue ready`)
    #[command(hide = true)]
    Ready,

    /// Link related issues (shortcut for `issue relate`)
    #[command(hide = true)]
    Relate {
        /// First issue ID
        id: i64,
        /// Second issue ID
        related: i64,
        /// Relation type (any string, e.g. related, assumption, falsifies, derived, caused-by)
        #[arg(short = 't', long = "type", default_value = "related")]
        relation_type: String,
    },

    /// Remove a relation (shortcut for `issue unrelate`)
    #[command(hide = true)]
    Unrelate {
        /// First issue ID
        id: i64,
        /// Second issue ID
        related: i64,
        /// Relation type to remove
        #[arg(short = 't', long = "type", default_value = "related")]
        relation_type: String,
    },

    /// List related issues (shortcut for `issue related`)
    #[command(hide = true)]
    Related {
        /// Issue ID
        id: i64,
    },

    /// Show falsification cascade — what breaks if this assumption is wrong
    Cascade {
        /// Issue ID to check cascade from
        id: i64,
    },

    /// Mark an assumption as falsified and propagate to downstream issues
    Falsify {
        /// Issue ID of the falsified assumption
        id: i64,
    },

    /// Suggest next issue (shortcut for `issue next`)
    #[command(hide = true)]
    Next,

    /// Show issue tree (shortcut for `issue tree`)
    #[command(hide = true)]
    Tree {
        /// Filter by status (open, closed, all)
        #[arg(short, long, default_value = "all")]
        status: String,
    },

    /// Mark tests as run (shortcut for `issue tested`)
    #[command(hide = true)]
    Tested,

    /// Start a timer (shortcut for `timer start`)
    #[command(hide = true, name = "start")]
    TimerStart {
        /// Issue ID
        id: i64,
    },

    /// Stop the timer (shortcut for `timer stop`)
    #[command(hide = true, name = "stop")]
    TimerStop,
}

// ============================================================================
// Issue subcommands (canonical path: `chainlink issue <command>`)
// ============================================================================

#[derive(Subcommand)]
enum IssueCommands {
    /// Create a new issue
    Create {
        /// Issue title
        title: String,
        /// Issue description
        #[arg(short, long)]
        description: Option<String>,
        /// Priority (low, medium, high, critical)
        #[arg(short, long, default_value = "medium")]
        priority: String,
        /// Template (bug, feature, refactor, research)
        #[arg(short, long)]
        template: Option<String>,
        /// Add labels to the issue
        #[arg(short, long)]
        label: Vec<String>,
        /// Set as current session work item
        #[arg(short, long)]
        work: bool,
    },

    /// Quick-create an issue and start working on it (create + label + session work)
    Quick {
        /// Issue title
        title: String,
        /// Issue description
        #[arg(short, long)]
        description: Option<String>,
        /// Priority (low, medium, high, critical)
        #[arg(short, long, default_value = "medium")]
        priority: String,
        /// Template (bug, feature, refactor, research)
        #[arg(short, long)]
        template: Option<String>,
        /// Add labels to the issue
        #[arg(short, long)]
        label: Vec<String>,
    },

    /// Create a subissue under a parent issue
    Subissue {
        /// Parent issue ID
        parent: i64,
        /// Subissue title
        title: String,
        /// Subissue description
        #[arg(short, long)]
        description: Option<String>,
        /// Priority (low, medium, high, critical)
        #[arg(short, long, default_value = "medium")]
        priority: String,
        /// Add labels to the subissue
        #[arg(short, long)]
        label: Vec<String>,
        /// Set as current session work item
        #[arg(short, long)]
        work: bool,
    },

    /// List issues
    List {
        /// Filter by status (open, closed, all)
        #[arg(short, long, default_value = "open")]
        status: String,
        /// Filter by label (repeatable, AND logic)
        #[arg(short, long)]
        label: Vec<String>,
        /// Filter by priority
        #[arg(short, long)]
        priority: Option<String>,
    },

    /// Search issues by text
    Search {
        /// Search query
        query: String,
    },

    /// Show issue details
    Show {
        /// Issue ID
        id: i64,
    },

    /// Update an issue
    Update {
        /// Issue ID
        id: i64,
        /// New title
        #[arg(short, long)]
        title: Option<String>,
        /// New description
        #[arg(short, long)]
        description: Option<String>,
        /// New priority
        #[arg(short, long)]
        priority: Option<String>,
    },

    /// Close an issue
    Close {
        /// Issue ID
        id: i64,
        /// Skip changelog entry
        #[arg(long)]
        no_changelog: bool,
    },

    /// Close all issues matching filters
    CloseAll {
        /// Filter by label (repeatable, AND logic)
        #[arg(short, long)]
        label: Vec<String>,
        /// Filter by priority
        #[arg(short, long)]
        priority: Option<String>,
        /// Skip changelog entries
        #[arg(long)]
        no_changelog: bool,
    },

    /// Reopen a closed issue
    Reopen {
        /// Issue ID
        id: i64,
    },

    /// Delete an issue
    Delete {
        /// Issue ID
        id: i64,
        /// Skip confirmation
        #[arg(short, long)]
        force: bool,
    },

    /// Add a comment to an issue
    Comment {
        /// Issue ID
        id: i64,
        /// Comment text
        text: String,
        /// Comment kind (note, plan, decision, observation, blocker, resolution, result, handoff, human)
        #[arg(long, default_value = "note")]
        kind: String,
    },

    /// Add a label to an issue
    Label {
        /// Issue ID
        id: i64,
        /// Label name
        label: String,
    },

    /// Remove a label from an issue
    Unlabel {
        /// Issue ID
        id: i64,
        /// Label name
        label: String,
    },

    /// Mark an issue as blocked by another
    Block {
        /// Issue ID that is blocked
        id: i64,
        /// Issue ID that is blocking
        blocker: i64,
    },

    /// Remove a blocking relationship
    Unblock {
        /// Issue ID that was blocked
        id: i64,
        /// Issue ID that was blocking
        blocker: i64,
    },

    /// List blocked issues
    Blocked,

    /// List issues ready to work on (no open blockers)
    Ready,

    /// Link two related issues
    Relate {
        /// First issue ID
        id: i64,
        /// Second issue ID
        related: i64,
        /// Relation type (any string, e.g. related, assumption, falsifies, derived, caused-by)
        #[arg(short = 't', long = "type", default_value = "related")]
        relation_type: String,
    },

    /// Remove a relation between issues
    Unrelate {
        /// First issue ID
        id: i64,
        /// Second issue ID
        related: i64,
        /// Relation type to remove
        #[arg(short = 't', long = "type", default_value = "related")]
        relation_type: String,
    },

    /// List related issues
    Related {
        /// Issue ID
        id: i64,
    },

    /// Show falsification cascade — what breaks if this assumption is wrong
    Cascade {
        /// Issue ID to check cascade from
        id: i64,
    },

    /// Mark an assumption as falsified and propagate to downstream issues
    Falsify {
        /// Issue ID of the falsified assumption
        id: i64,
    },

    /// Suggest the next issue to work on
    Next,

    /// Show issues as a tree hierarchy
    Tree {
        /// Filter by status (open, closed, all)
        #[arg(short, long, default_value = "all")]
        status: String,
    },

    /// Mark tests as run (resets test reminder)
    Tested,
}

// ============================================================================
// Timer subcommands (canonical path: `chainlink timer <command>`)
// ============================================================================

#[derive(Subcommand)]
enum TimerCommands {
    /// Start a timer for an issue
    Start {
        /// Issue ID
        id: i64,
    },
    /// Stop the current timer
    Stop,
    /// Show current timer status
    Show,
}

// ============================================================================
// Other subcommand enums
// ============================================================================

#[derive(Subcommand)]
enum UsageCommands {
    /// Record a token usage entry
    Record {
        /// Agent ID
        #[arg(long)]
        agent: String,
        /// Model name (e.g., claude-opus-4-6)
        #[arg(long)]
        model: String,
        /// Input tokens consumed
        #[arg(long)]
        input_tokens: i64,
        /// Output tokens produced
        #[arg(long)]
        output_tokens: i64,
        /// Cache read tokens
        #[arg(long)]
        cache_read: Option<i64>,
        /// Cache creation tokens
        #[arg(long)]
        cache_creation: Option<i64>,
        /// Session ID to associate with
        #[arg(long)]
        session: Option<i64>,
    },
    /// Show details of a single usage record
    Show {
        /// Usage record ID
        id: i64,
    },
    /// List usage records
    List {
        /// Filter by agent ID
        #[arg(long)]
        agent: Option<String>,
        /// Filter by model
        #[arg(long)]
        model: Option<String>,
        /// Max records to show
        #[arg(long, default_value = "20")]
        limit: i64,
    },
    /// Show aggregated usage summary
    Summary {
        /// Filter by agent ID
        #[arg(long)]
        agent: Option<String>,
    },
}

#[derive(Subcommand)]
enum ArchiveCommands {
    /// Archive a closed issue
    Add {
        /// Issue ID
        id: i64,
    },
    /// Unarchive an issue (restore to closed)
    Remove {
        /// Issue ID
        id: i64,
    },
    /// List archived issues
    List,
    /// Archive all issues closed more than N days ago
    Older {
        /// Days threshold
        days: i64,
    },
}

#[derive(Subcommand)]
enum MilestoneCommands {
    /// Create a new milestone
    Create {
        /// Milestone name
        name: String,
        /// Description
        #[arg(short, long)]
        description: Option<String>,
    },
    /// List milestones
    List {
        /// Filter by status (open, closed, all)
        #[arg(short, long, default_value = "open")]
        status: String,
    },
    /// Show milestone details
    Show {
        /// Milestone ID
        id: i64,
    },
    /// Add issues to a milestone
    Add {
        /// Milestone ID
        id: i64,
        /// Issue IDs to add
        issues: Vec<i64>,
    },
    /// Remove an issue from a milestone
    Remove {
        /// Milestone ID
        id: i64,
        /// Issue ID to remove
        issue: i64,
    },
    /// Close a milestone
    Close {
        /// Milestone ID
        id: i64,
    },
    /// Delete a milestone
    Delete {
        /// Milestone ID
        id: i64,
    },
}

#[derive(Subcommand)]
enum SessionCommands {
    /// Start a new session
    Start,
    /// End the current session
    End {
        /// Handoff notes for the next session
        #[arg(short, long)]
        notes: Option<String>,
    },
    /// Show current session status
    Status,
    /// Set the issue being worked on
    Work {
        /// Issue ID
        id: i64,
    },
    /// Show handoff notes from the previous session
    LastHandoff,
    /// Record last action for context compression breadcrumbs
    Action {
        /// Description of what you just did or are doing
        text: String,
    },
}

#[derive(Subcommand)]
enum CpitdCommands {
    /// Scan for code clones and create issues
    Scan {
        /// Paths to scan (defaults to current directory)
        paths: Vec<String>,
        /// Minimum token sequence length to report
        #[arg(long, default_value = "50")]
        min_tokens: u32,
        /// Glob patterns to exclude (repeatable)
        #[arg(long)]
        ignore: Vec<String>,
        /// Show what would be created without creating issues
        #[arg(long)]
        dry_run: bool,
    },
    /// Show open clone issues
    Status,
    /// Close all open clone issues
    Clear,
}

#[derive(Subcommand)]
enum AgentCommands {
    /// Initialize agent identity for this machine
    Init {
        /// Agent ID (e.g. "worker-1")
        agent_id: String,
        /// Description of this agent
        #[arg(short, long)]
        description: Option<String>,
        /// Overwrite existing configuration
        #[arg(short, long)]
        force: bool,
    },
    /// Show current agent identity and lock status
    Status,
}

#[derive(Subcommand)]
enum LocksCommands {
    /// List all active locks
    List,
    /// Check if a specific issue is locked
    Check {
        /// Issue ID
        id: i64,
    },
    /// Claim a lock on an issue
    Claim {
        /// Issue ID
        id: i64,
        /// Branch name associated with the lock
        #[arg(short, long)]
        branch: Option<String>,
    },
    /// Release a lock on an issue
    Release {
        /// Issue ID
        id: i64,
    },
    /// Force-steal a stale lock
    Steal {
        /// Issue ID
        id: i64,
    },
}

#[derive(Subcommand)]
enum DaemonCommands {
    /// Start the background daemon
    Start,
    /// Stop the background daemon
    Stop,
    /// Check daemon status
    Status,
    /// Internal: run the daemon loop (used by start)
    #[command(hide = true)]
    Run {
        #[arg(long)]
        dir: PathBuf,
    },
}

// ============================================================================
// Helpers
// ============================================================================

fn find_chainlink_dir() -> Result<PathBuf> {
    let mut current = env::current_dir()?;

    loop {
        let candidate = current.join(".chainlink");
        if candidate.is_dir() {
            return Ok(candidate);
        }

        if !current.pop() {
            bail!("Not a chainlink repository (or any parent). Run 'chainlink init' first.");
        }
    }
}

fn get_db() -> Result<Database> {
    let chainlink_dir = find_chainlink_dir()?;
    let db_path = chainlink_dir.join("issues.db");
    Database::open(&db_path).context("Failed to open database")
}

fn init_tracing(log_level: &str, log_format: &str) {
    use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("warn"));
    if log_format == "json" {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().json().with_writer(std::io::stderr))
            .init();
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt::layer().with_target(false).with_writer(std::io::stderr))
            .init();
    }
}

// ============================================================================
// Dispatch helpers for canonical subcommands
// ============================================================================

fn dispatch_issue(action: IssueCommands, quiet: bool, json: bool) -> Result<()> {
    match action {
        IssueCommands::Create {
            title,
            description,
            priority,
            template,
            label,
            work,
        } => {
            let db = get_db()?;
            let chainlink_dir = find_chainlink_dir().ok();
            let opts = commands::create::CreateOpts {
                labels: &label,
                work,
                quiet,
                chainlink_dir: chainlink_dir.as_deref(),
            };
            commands::create::run(
                &db,
                &title,
                description.as_deref(),
                &priority,
                template.as_deref(),
                &opts,
            )
        }

        IssueCommands::Quick {
            title,
            description,
            priority,
            template,
            label,
        } => {
            let db = get_db()?;
            let chainlink_dir = find_chainlink_dir().ok();
            let opts = commands::create::CreateOpts {
                labels: &label,
                work: true,
                quiet,
                chainlink_dir: chainlink_dir.as_deref(),
            };
            commands::create::run(
                &db,
                &title,
                description.as_deref(),
                &priority,
                template.as_deref(),
                &opts,
            )
        }

        IssueCommands::Subissue {
            parent,
            title,
            description,
            priority,
            label,
            work,
        } => {
            let db = get_db()?;
            let chainlink_dir = find_chainlink_dir().ok();
            let opts = commands::create::CreateOpts {
                labels: &label,
                work,
                quiet,
                chainlink_dir: chainlink_dir.as_deref(),
            };
            commands::create::run_subissue(
                &db,
                parent,
                &title,
                description.as_deref(),
                &priority,
                &opts,
            )
        }

        IssueCommands::List {
            status,
            label,
            priority,
        } => {
            let db = get_db()?;
            if json {
                commands::list::run_json(&db, Some(&status), &label, priority.as_deref())
            } else {
                commands::list::run(&db, Some(&status), &label, priority.as_deref())
            }
        }

        IssueCommands::Search { query } => {
            let db = get_db()?;
            if json {
                commands::search::run_json(&db, &query)
            } else {
                commands::search::run(&db, &query)
            }
        }

        IssueCommands::Show { id } => {
            let db = get_db()?;
            if json {
                commands::show::run_json(&db, id)
            } else {
                commands::show::run(&db, id)
            }
        }

        IssueCommands::Update {
            id,
            title,
            description,
            priority,
        } => {
            let db = get_db()?;
            commands::update::run(
                &db,
                id,
                title.as_deref(),
                description.as_deref(),
                priority.as_deref(),
            )
        }

        IssueCommands::Close { id, no_changelog } => {
            let db = get_db()?;
            let chainlink_dir = find_chainlink_dir()?;
            if quiet {
                commands::status::close_quiet(&db, id, !no_changelog, &chainlink_dir)
            } else {
                commands::status::close(&db, id, !no_changelog, &chainlink_dir)
            }
        }

        IssueCommands::CloseAll {
            label,
            priority,
            no_changelog,
        } => {
            let db = get_db()?;
            let chainlink_dir = find_chainlink_dir()?;
            commands::status::close_all(
                &db,
                &label,
                priority.as_deref(),
                !no_changelog,
                &chainlink_dir,
            )
        }

        IssueCommands::Reopen { id } => {
            let db = get_db()?;
            commands::status::reopen(&db, id)
        }

        IssueCommands::Delete { id, force } => {
            let db = get_db()?;
            commands::delete::run(&db, id, force)
        }

        IssueCommands::Comment { id, text, kind } => {
            let db = get_db()?;
            commands::comment::run(&db, id, &text, &kind)
        }

        IssueCommands::Label { id, label } => {
            let db = get_db()?;
            commands::label::add(&db, id, &label)
        }

        IssueCommands::Unlabel { id, label } => {
            let db = get_db()?;
            commands::label::remove(&db, id, &label)
        }

        IssueCommands::Block { id, blocker } => {
            let db = get_db()?;
            commands::deps::block(&db, id, blocker)
        }

        IssueCommands::Unblock { id, blocker } => {
            let db = get_db()?;
            commands::deps::unblock(&db, id, blocker)
        }

        IssueCommands::Blocked => {
            let db = get_db()?;
            commands::deps::list_blocked(&db)
        }

        IssueCommands::Ready => {
            let db = get_db()?;
            commands::deps::list_ready(&db)
        }

        IssueCommands::Relate {
            id,
            related,
            relation_type,
        } => {
            let db = get_db()?;
            commands::relate::add_typed(&db, id, related, &relation_type)
        }

        IssueCommands::Unrelate {
            id,
            related,
            relation_type,
        } => {
            let db = get_db()?;
            commands::relate::remove_typed(&db, id, related, &relation_type)
        }

        IssueCommands::Related { id } => {
            let db = get_db()?;
            commands::relate::list(&db, id)
        }

        IssueCommands::Cascade { id } => {
            let db = get_db()?;
            commands::relate::cascade(&db, id)
        }

        IssueCommands::Falsify { id } => {
            let db = get_db()?;
            commands::relate::falsify(&db, id)
        }

        IssueCommands::Next => {
            let db = get_db()?;
            let chainlink_dir = find_chainlink_dir()?;
            commands::next::run(&db, &chainlink_dir)
        }

        IssueCommands::Tree { status } => {
            let db = get_db()?;
            commands::tree::run(&db, Some(&status))
        }

        IssueCommands::Tested => {
            let chainlink_dir = find_chainlink_dir()?;
            commands::tested::run(&chainlink_dir)
        }
    }
}

fn dispatch_timer(action: Option<TimerCommands>) -> Result<()> {
    let db = get_db()?;
    match action {
        Some(TimerCommands::Start { id }) => commands::timer::start(&db, id),
        Some(TimerCommands::Stop) => commands::timer::stop(&db),
        Some(TimerCommands::Show) | None => commands::timer::status(&db),
    }
}

// ============================================================================
// Main
// ============================================================================

fn main() -> Result<()> {
    // The Commands enum is large (many hidden aliases for backward compat).
    // In debug builds the unoptimized match overflows the default 1 MB stack
    // on Windows, so we run the real logic on a thread with a 4 MB stack.
    let result = std::thread::Builder::new()
        .name("main".into())
        .stack_size(4 * 1024 * 1024)
        .spawn(run)
        .map_err(|e| anyhow::anyhow!("failed to spawn main thread: {e}"))?
        .join();

    match result {
        Ok(r) => r,
        Err(e) => std::panic::resume_unwind(e),
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log_level, &cli.log_format);
    let quiet = cli.quiet;
    let json = cli.json;

    match cli.command {
        Commands::Init { force } => {
            let cwd = env::current_dir()?;
            commands::init::run(&cwd, force)
        }

        // ====== Canonical hierarchical commands ======
        Commands::Issue { action } => dispatch_issue(action, quiet, json),
        Commands::Timer { action } => dispatch_timer(action),

        // ====== Hidden backward-compatible aliases ======
        Commands::Create {
            title,
            description,
            priority,
            template,
            label,
            work,
        } => dispatch_issue(
            IssueCommands::Create {
                title,
                description,
                priority,
                template,
                label,
                work,
            },
            quiet,
            json,
        ),

        Commands::Quick {
            title,
            description,
            priority,
            template,
            label,
        } => dispatch_issue(
            IssueCommands::Quick {
                title,
                description,
                priority,
                template,
                label,
            },
            quiet,
            json,
        ),

        Commands::Subissue {
            parent,
            title,
            description,
            priority,
            label,
            work,
        } => dispatch_issue(
            IssueCommands::Subissue {
                parent,
                title,
                description,
                priority,
                label,
                work,
            },
            quiet,
            json,
        ),

        Commands::List {
            status,
            label,
            priority,
        } => dispatch_issue(
            IssueCommands::List {
                status,
                label,
                priority,
            },
            quiet,
            json,
        ),

        Commands::Search { query } => dispatch_issue(IssueCommands::Search { query }, quiet, json),

        Commands::Show { id } => dispatch_issue(IssueCommands::Show { id }, quiet, json),

        Commands::Update {
            id,
            title,
            description,
            priority,
        } => dispatch_issue(
            IssueCommands::Update {
                id,
                title,
                description,
                priority,
            },
            quiet,
            json,
        ),

        Commands::Close { id, no_changelog } => {
            dispatch_issue(IssueCommands::Close { id, no_changelog }, quiet, json)
        }

        Commands::CloseAll {
            label,
            priority,
            no_changelog,
        } => dispatch_issue(
            IssueCommands::CloseAll {
                label,
                priority,
                no_changelog,
            },
            quiet,
            json,
        ),

        Commands::Reopen { id } => dispatch_issue(IssueCommands::Reopen { id }, quiet, json),

        Commands::Delete { id, force } => {
            dispatch_issue(IssueCommands::Delete { id, force }, quiet, json)
        }

        Commands::Comment { id, text, kind } => {
            dispatch_issue(IssueCommands::Comment { id, text, kind }, quiet, json)
        }

        Commands::Label { id, label } => {
            dispatch_issue(IssueCommands::Label { id, label }, quiet, json)
        }

        Commands::Unlabel { id, label } => {
            dispatch_issue(IssueCommands::Unlabel { id, label }, quiet, json)
        }

        Commands::Block { id, blocker } => {
            dispatch_issue(IssueCommands::Block { id, blocker }, quiet, json)
        }

        Commands::Unblock { id, blocker } => {
            dispatch_issue(IssueCommands::Unblock { id, blocker }, quiet, json)
        }

        Commands::Blocked => dispatch_issue(IssueCommands::Blocked, quiet, json),

        Commands::Ready => dispatch_issue(IssueCommands::Ready, quiet, json),

        Commands::Relate {
            id,
            related,
            relation_type,
        } => dispatch_issue(
            IssueCommands::Relate {
                id,
                related,
                relation_type,
            },
            quiet,
            json,
        ),

        Commands::Unrelate {
            id,
            related,
            relation_type,
        } => dispatch_issue(
            IssueCommands::Unrelate {
                id,
                related,
                relation_type,
            },
            quiet,
            json,
        ),

        Commands::Related { id } => dispatch_issue(IssueCommands::Related { id }, quiet, json),

        Commands::Cascade { id } => dispatch_issue(IssueCommands::Cascade { id }, quiet, json),

        Commands::Falsify { id } => dispatch_issue(IssueCommands::Falsify { id }, quiet, json),

        Commands::Next => dispatch_issue(IssueCommands::Next, quiet, json),

        Commands::Tree { status } => dispatch_issue(IssueCommands::Tree { status }, quiet, json),

        Commands::Tested => dispatch_issue(IssueCommands::Tested, quiet, json),

        Commands::TimerStart { id } => dispatch_timer(Some(TimerCommands::Start { id })),

        Commands::TimerStop => dispatch_timer(Some(TimerCommands::Stop)),

        // ====== Non-issue, non-timer commands ======
        Commands::Export { output, format } => {
            let db = get_db()?;
            match format.as_str() {
                "json" => commands::export::run_json(&db, output.as_deref()),
                "markdown" | "md" => commands::export::run_markdown(&db, output.as_deref()),
                _ => {
                    bail!("Unknown format '{}'. Use 'json' or 'markdown'", format);
                }
            }
        }

        Commands::Import { input } => {
            let db = get_db()?;
            let path = std::path::Path::new(&input);
            commands::import::run_json(&db, path)
        }

        Commands::Archive { action } => {
            let db = get_db()?;
            match action {
                ArchiveCommands::Add { id } => commands::archive::archive(&db, id),
                ArchiveCommands::Remove { id } => commands::archive::unarchive(&db, id),
                ArchiveCommands::List => commands::archive::list(&db),
                ArchiveCommands::Older { days } => commands::archive::archive_older(&db, days),
            }
        }

        Commands::Milestone { action } => {
            let db = get_db()?;
            match action {
                MilestoneCommands::Create { name, description } => {
                    commands::milestone::create(&db, &name, description.as_deref())
                }
                MilestoneCommands::List { status } => commands::milestone::list(&db, Some(&status)),
                MilestoneCommands::Show { id } => commands::milestone::show(&db, id),
                MilestoneCommands::Add { id, issues } => commands::milestone::add(&db, id, &issues),
                MilestoneCommands::Remove { id, issue } => {
                    commands::milestone::remove(&db, id, issue)
                }
                MilestoneCommands::Close { id } => commands::milestone::close(&db, id),
                MilestoneCommands::Delete { id } => commands::milestone::delete(&db, id),
            }
        }

        Commands::Session { action } => {
            let db = get_db()?;
            match action {
                SessionCommands::Start => {
                    let chainlink_dir = find_chainlink_dir()?;
                    commands::session::start(&db, &chainlink_dir)
                }
                SessionCommands::End { notes } => commands::session::end(&db, notes.as_deref()),
                SessionCommands::Status => {
                    if json {
                        commands::session::status_json(&db)
                    } else {
                        commands::session::status(&db)
                    }
                }
                SessionCommands::Work { id } => {
                    let chainlink_dir = find_chainlink_dir()?;
                    commands::session::work(&db, id, &chainlink_dir)
                }
                SessionCommands::LastHandoff => commands::session::last_handoff(&db),
                SessionCommands::Action { text } => commands::session::action(&db, &text),
            }
        }

        Commands::Daemon { action } => match action {
            DaemonCommands::Start => {
                let chainlink_dir = find_chainlink_dir()?;
                daemon::start(&chainlink_dir)
            }
            DaemonCommands::Stop => {
                let chainlink_dir = find_chainlink_dir()?;
                daemon::stop(&chainlink_dir)
            }
            DaemonCommands::Status => {
                let chainlink_dir = find_chainlink_dir()?;
                daemon::status(&chainlink_dir)
            }
            DaemonCommands::Run { dir } => daemon::run_daemon(&dir),
        },

        Commands::Cpitd { action } => {
            let db = get_db()?;
            match action {
                CpitdCommands::Scan {
                    paths,
                    min_tokens,
                    ignore,
                    dry_run,
                } => commands::cpitd::scan(&db, &paths, min_tokens, &ignore, dry_run, quiet),
                CpitdCommands::Status => commands::cpitd::status(&db),
                CpitdCommands::Clear => commands::cpitd::clear(&db),
            }
        }

        Commands::Usage { action } => {
            let db = get_db()?;
            match action {
                UsageCommands::Record {
                    agent,
                    model,
                    input_tokens,
                    output_tokens,
                    cache_read,
                    cache_creation,
                    session,
                } => {
                    let raw = token_usage::RawTokenUsage {
                        input_tokens,
                        output_tokens,
                        cache_read_input_tokens: cache_read,
                        cache_creation_input_tokens: cache_creation,
                    };
                    let parsed = token_usage::parse_api_usage(&raw, &model, &agent, session);
                    commands::usage::record(&db, &parsed)
                }
                UsageCommands::Show { id } => commands::usage::show(&db, id, json),
                UsageCommands::List {
                    agent,
                    model,
                    limit,
                } => commands::usage::list(
                    &db,
                    agent.as_deref(),
                    model.as_deref(),
                    Some(limit),
                    json,
                ),
                UsageCommands::Summary { agent } => {
                    commands::usage::summary(&db, agent.as_deref(), json)
                }
            }
        }

        Commands::Agent { action } => {
            let chainlink_dir = find_chainlink_dir()?;
            match action {
                AgentCommands::Init {
                    agent_id,
                    description,
                    force,
                } => {
                    commands::agent::init(&chainlink_dir, &agent_id, description.as_deref(), force)
                }
                AgentCommands::Status => commands::agent::status(&chainlink_dir),
            }
        }

        Commands::Locks { action } => {
            let chainlink_dir = find_chainlink_dir()?;
            match action {
                LocksCommands::List => {
                    let db = get_db()?;
                    commands::locks_cmd::list(&chainlink_dir, &db, json)
                }
                LocksCommands::Check { id } => commands::locks_cmd::check(&chainlink_dir, id),
                LocksCommands::Claim { id, branch } => {
                    commands::locks_cmd::claim(&chainlink_dir, id, branch.as_deref())
                }
                LocksCommands::Release { id } => commands::locks_cmd::release(&chainlink_dir, id),
                LocksCommands::Steal { id } => commands::locks_cmd::steal(&chainlink_dir, id),
            }
        }

        Commands::Sync => {
            let chainlink_dir = find_chainlink_dir()?;
            commands::locks_cmd::sync_cmd(&chainlink_dir)
        }
    }
}
