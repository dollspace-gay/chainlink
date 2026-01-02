use anyhow::{bail, Result};

use crate::db::Database;

const VALID_PRIORITIES: [&str; 4] = ["low", "medium", "high", "critical"];

/// Built-in issue templates
pub struct Template {
    pub name: &'static str,
    pub priority: &'static str,
    pub label: &'static str,
    pub description_prefix: Option<&'static str>,
}

pub const TEMPLATES: &[Template] = &[
    Template {
        name: "bug",
        priority: "high",
        label: "bug",
        description_prefix: Some("Steps to reproduce:\n1. \n\nExpected: \nActual: "),
    },
    Template {
        name: "feature",
        priority: "medium",
        label: "feature",
        description_prefix: Some("Goal: \n\nAcceptance criteria:\n- "),
    },
    Template {
        name: "refactor",
        priority: "low",
        label: "refactor",
        description_prefix: Some("Current state: \n\nDesired state: \n\nReason: "),
    },
    Template {
        name: "research",
        priority: "low",
        label: "research",
        description_prefix: Some("Question: \n\nContext: \n\nFindings: "),
    },
];

pub fn get_template(name: &str) -> Option<&'static Template> {
    TEMPLATES.iter().find(|t| t.name == name)
}

pub fn list_templates() -> Vec<&'static str> {
    TEMPLATES.iter().map(|t| t.name).collect()
}

pub fn validate_priority(priority: &str) -> bool {
    VALID_PRIORITIES.contains(&priority)
}

pub fn run(
    db: &Database,
    title: &str,
    description: Option<&str>,
    priority: &str,
    template: Option<&str>,
) -> Result<()> {
    // Apply template if specified
    let (final_priority, final_description, label) = if let Some(tmpl_name) = template {
        let tmpl = get_template(tmpl_name).ok_or_else(|| {
            anyhow::anyhow!(
                "Unknown template '{}'. Available: {}",
                tmpl_name,
                list_templates().join(", ")
            )
        })?;

        // Template priority is default, user can override
        let priority = if priority != "medium" {
            priority
        } else {
            tmpl.priority
        };

        // Combine template description prefix with user description
        let desc = match (tmpl.description_prefix, description) {
            (Some(prefix), Some(user_desc)) => Some(format!("{}\n\n{}", prefix, user_desc)),
            (Some(prefix), None) => Some(prefix.to_string()),
            (None, user_desc) => user_desc.map(|s| s.to_string()),
        };

        (priority.to_string(), desc, Some(tmpl.label))
    } else {
        (
            priority.to_string(),
            description.map(|s| s.to_string()),
            None,
        )
    };

    if !validate_priority(&final_priority) {
        bail!(
            "Invalid priority '{}'. Must be one of: {}",
            final_priority,
            VALID_PRIORITIES.join(", ")
        );
    }

    let id = db.create_issue(title, final_description.as_deref(), &final_priority)?;

    // Auto-add label from template
    if let Some(lbl) = label {
        db.add_label(id, lbl)?;
    }

    println!("Created issue #{}", id);
    if let Some(tmpl) = template {
        println!("  Applied template: {}", tmpl);
    }
    Ok(())
}

pub fn run_subissue(
    db: &Database,
    parent_id: i64,
    title: &str,
    description: Option<&str>,
    priority: &str,
) -> Result<()> {
    if !validate_priority(priority) {
        bail!(
            "Invalid priority '{}'. Must be one of: {}",
            priority,
            VALID_PRIORITIES.join(", ")
        );
    }

    // Verify parent exists
    let parent = db.get_issue(parent_id)?;
    if parent.is_none() {
        bail!("Parent issue #{} not found", parent_id);
    }

    let id = db.create_subissue(parent_id, title, description, priority)?;
    println!("Created subissue #{} under #{}", id, parent_id);
    Ok(())
}
