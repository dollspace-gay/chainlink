#!/usr/bin/env python3
"""
Chainlink behavioral hook for Claude Code.
Injects best practice reminders on every prompt submission.
"""

import json
import sys
import os
from datetime import datetime

# Detect language from common file extensions in the working directory
def detect_languages():
    """Scan for common source files to determine active languages."""
    extensions = {
        '.rs': 'Rust',
        '.py': 'Python',
        '.js': 'JavaScript',
        '.ts': 'TypeScript',
        '.tsx': 'TypeScript/React',
        '.jsx': 'JavaScript/React',
        '.go': 'Go',
        '.java': 'Java',
        '.c': 'C',
        '.cpp': 'C++',
        '.cs': 'C#',
        '.rb': 'Ruby',
        '.php': 'PHP',
        '.swift': 'Swift',
        '.kt': 'Kotlin',
        '.scala': 'Scala',
        '.zig': 'Zig',
        '.odin': 'Odin',
    }

    found = set()
    cwd = os.getcwd()

    # Quick scan of src/ and current directory
    scan_dirs = [cwd]
    src_dir = os.path.join(cwd, 'src')
    if os.path.isdir(src_dir):
        scan_dirs.append(src_dir)

    for scan_dir in scan_dirs:
        try:
            for entry in os.listdir(scan_dir):
                ext = os.path.splitext(entry)[1].lower()
                if ext in extensions:
                    found.add(extensions[ext])
        except (PermissionError, OSError):
            pass

    return list(found) if found else ['the project']


LANGUAGE_PRACTICES = {
    'Rust': """
- Use `?` operator, not `.unwrap()` - propagate errors with `.context()`
- Prefer `&str` params, `String` for owned data
- Use `clippy` and `rustfmt`
- Parameterized SQL queries only (rusqlite `params![]`)
- No `unsafe` without explicit justification""",

    'Python': """
- Use type hints for function signatures
- Handle exceptions properly, don't bare `except:`
- Use `pathlib` for file paths
- Use context managers (`with`) for resources
- Parameterized queries for SQL (never f-strings)""",

    'JavaScript': """
- Use `const`/`let`, never `var`
- Proper error handling with try/catch
- Use async/await over raw promises where cleaner
- Validate all user input
- Use parameterized queries for databases""",

    'TypeScript': """
- Use strict mode, avoid `any` type
- Define proper interfaces/types
- Use `const`/`let`, never `var`
- Proper error handling with try/catch
- Validate all external data at boundaries""",

    'Go': """
- Always check returned errors
- Use `context.Context` for cancellation
- Prefer composition over inheritance
- Use `defer` for cleanup
- Validate input, especially from external sources""",
}


def get_language_section(languages):
    """Build language-specific best practices section."""
    sections = []
    for lang in languages:
        if lang in LANGUAGE_PRACTICES:
            sections.append(f"### {lang} Best Practices{LANGUAGE_PRACTICES[lang]}")

    if not sections:
        return ""

    return "\n\n".join(sections)


def build_reminder(languages):
    """Build the full reminder context."""
    lang_section = get_language_section(languages)
    lang_list = ", ".join(languages) if languages else "this project"
    current_year = datetime.now().year

    reminder = f"""<chainlink-behavioral-guard>
## Code Quality Requirements

You are working on a {lang_list} project. Follow these requirements strictly:

### Pre-Coding Grounding (PREVENT HALLUCINATIONS)
Before writing code that uses external libraries, APIs, or unfamiliar patterns:
1. **VERIFY IT EXISTS**: Use WebSearch to confirm the crate/package/module exists and check its actual API
2. **CHECK THE DOCS**: Fetch documentation to see real function signatures, not imagined ones
3. **CONFIRM SYNTAX**: If unsure about language features or library usage, search first
4. **USE LATEST VERSIONS**: Always check for and use the latest stable version of dependencies (security + features)
5. **NO GUESSING**: If you can't verify it, tell the user you need to research it

Examples of when to search:
- Using a crate/package you haven't used recently → search "[package] [language] docs {current_year}"
- Uncertain about function parameters → search for actual API reference
- New language feature or syntax → verify it exists in the version being used
- System calls or platform-specific code → confirm the correct API
- Adding a dependency → search "[package] latest version {current_year}" to get current release

### General Requirements
1. **NO STUBS**: Implement complete, working code. Never write placeholder functions or TODO comments as implementation.
2. **NO DEAD CODE**: Discover if dead code is truly dead or if it's an incomplete feature. If incomplete, complete it. If truly dead, remove it.
3. **FULL FEATURES**: Implement the complete feature as requested. Don't stop partway or suggest "you could add X later."
4. **ERROR HANDLING**: Proper error handling everywhere. No panics/crashes on bad input.
5. **SECURITY**: Validate input, use parameterized queries, no command injection, no hardcoded secrets.
6. **READ BEFORE WRITE**: Always read a file before editing it. Never guess at contents.
{lang_section}

### Large File Management (500+ lines)
If you need to write or modify code that will exceed 500 lines:
1. Create a parent issue for the overall feature: `chainlink create "<feature name>" -p high`
2. Break down into subissues: `chainlink subissue <parent_id> "<component 1>"`, etc.
3. Inform the user: "This implementation will require multiple files/components. I've created issue #X with Y subissues to track progress."
4. Work on one subissue at a time, marking each complete before moving on.

### Context Window Management
If the conversation is getting long OR the task requires many more steps:
1. Create a chainlink issue to track remaining work: `chainlink create "Continue: <task summary>" -p high`
2. Add detailed notes as a comment: `chainlink comment <id> "<what's done, what's next>"`
3. Inform the user: "This task will require additional turns. I've created issue #X to track progress."

Use `chainlink session work <id>` to mark what you're working on.
</chainlink-behavioral-guard>"""

    return reminder


def main():
    try:
        # Read input from stdin (Claude Code passes prompt info)
        input_data = json.load(sys.stdin)
    except json.JSONDecodeError:
        # If no valid JSON, still inject reminder
        pass
    except Exception:
        pass

    # Detect languages in the project
    languages = detect_languages()

    # Output the reminder as plain text (gets injected as context)
    print(build_reminder(languages))
    sys.exit(0)


if __name__ == "__main__":
    main()
