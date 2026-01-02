mod commands;
mod daemon;
mod db;
mod models;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use std::env;
use std::path::PathBuf;

use db::Database;

#[derive(Parser)]
#[command(name = "chainlink")]
#[command(about = "A simple, lean issue tracker CLI")]
#[command(version)]
struct Cli {
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
    },

    /// List issues
    List {
        /// Filter by status (open, closed, all)
        #[arg(short, long, default_value = "open")]
        status: String,
        /// Filter by label
        #[arg(short, long)]
        label: Option<String>,
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
    },

    /// Remove a relation between issues
    Unrelate {
        /// First issue ID
        id: i64,
        /// Second issue ID
        related: i64,
    },

    /// List related issues
    Related {
        /// Issue ID
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

    /// Start a timer for an issue
    Start {
        /// Issue ID
        id: i64,
    },

    /// Stop the current timer
    Stop,

    /// Show current timer status
    Timer,

    /// Mark tests as run (resets test reminder)
    Tested,

    /// Export issues to file
    Export {
        /// Output file path
        output: String,
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

fn find_chainlink_dir() -> Result<PathBuf> {
    let mut current = env::current_dir()?;

    loop {
        let candidate = current.join(".chainlink");
        if candidate.exists() && candidate.is_dir() {
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

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init { force } => {
            let cwd = env::current_dir()?;
            commands::init::run(&cwd, force)
        }

        Commands::Create {
            title,
            description,
            priority,
            template,
        } => {
            let db = get_db()?;
            commands::create::run(
                &db,
                &title,
                description.as_deref(),
                &priority,
                template.as_deref(),
            )
        }

        Commands::Subissue {
            parent,
            title,
            description,
            priority,
        } => {
            let db = get_db()?;
            commands::create::run_subissue(&db, parent, &title, description.as_deref(), &priority)
        }

        Commands::List {
            status,
            label,
            priority,
        } => {
            let db = get_db()?;
            commands::list::run(&db, Some(&status), label.as_deref(), priority.as_deref())
        }

        Commands::Search { query } => {
            let db = get_db()?;
            commands::search::run(&db, &query)
        }

        Commands::Show { id } => {
            let db = get_db()?;
            commands::show::run(&db, id)
        }

        Commands::Update {
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

        Commands::Close { id } => {
            let db = get_db()?;
            commands::status::close(&db, id)
        }

        Commands::Reopen { id } => {
            let db = get_db()?;
            commands::status::reopen(&db, id)
        }

        Commands::Delete { id, force } => {
            let db = get_db()?;
            commands::delete::run(&db, id, force)
        }

        Commands::Comment { id, text } => {
            let db = get_db()?;
            commands::comment::run(&db, id, &text)
        }

        Commands::Label { id, label } => {
            let db = get_db()?;
            commands::label::add(&db, id, &label)
        }

        Commands::Unlabel { id, label } => {
            let db = get_db()?;
            commands::label::remove(&db, id, &label)
        }

        Commands::Block { id, blocker } => {
            let db = get_db()?;
            commands::deps::block(&db, id, blocker)
        }

        Commands::Unblock { id, blocker } => {
            let db = get_db()?;
            commands::deps::unblock(&db, id, blocker)
        }

        Commands::Blocked => {
            let db = get_db()?;
            commands::deps::list_blocked(&db)
        }

        Commands::Ready => {
            let db = get_db()?;
            commands::deps::list_ready(&db)
        }

        Commands::Relate { id, related } => {
            let db = get_db()?;
            commands::relate::add(&db, id, related)
        }

        Commands::Unrelate { id, related } => {
            let db = get_db()?;
            commands::relate::remove(&db, id, related)
        }

        Commands::Related { id } => {
            let db = get_db()?;
            commands::relate::list(&db, id)
        }

        Commands::Next => {
            let db = get_db()?;
            commands::next::run(&db)
        }

        Commands::Tree { status } => {
            let db = get_db()?;
            commands::tree::run(&db, Some(&status))
        }

        Commands::Start { id } => {
            let db = get_db()?;
            commands::timer::start(&db, id)
        }

        Commands::Stop => {
            let db = get_db()?;
            commands::timer::stop(&db)
        }

        Commands::Timer => {
            let db = get_db()?;
            commands::timer::status(&db)
        }

        Commands::Tested => {
            let chainlink_dir = find_chainlink_dir()?;
            commands::tested::run(&chainlink_dir)
        }

        Commands::Export { output, format } => {
            let db = get_db()?;
            let path = std::path::Path::new(&output);
            match format.as_str() {
                "json" => commands::export::run_json(&db, path),
                "markdown" | "md" => commands::export::run_markdown(&db, path),
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
                SessionCommands::Start => commands::session::start(&db),
                SessionCommands::End { notes } => commands::session::end(&db, notes.as_deref()),
                SessionCommands::Status => commands::session::status(&db),
                SessionCommands::Work { id } => commands::session::work(&db, id),
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
    }
}
