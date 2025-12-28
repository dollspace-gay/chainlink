#!/usr/bin/env python3
"""
Post-edit hook that detects stub patterns, runs linters, and reminds about tests.
Runs after Write/Edit tool usage.
"""

import json
import sys
import os
import re
import subprocess
import glob
import time

# Stub patterns to detect (compiled regex for performance)
STUB_PATTERNS = [
    (r'\bTODO\b', 'TODO comment'),
    (r'\bFIXME\b', 'FIXME comment'),
    (r'\bXXX\b', 'XXX marker'),
    (r'\bHACK\b', 'HACK marker'),
    (r'^\s*pass\s*$', 'bare pass statement'),
    (r'^\s*\.\.\.\s*$', 'ellipsis placeholder'),
    (r'\bunimplemented!\s*\(\s*\)', 'unimplemented!() macro'),
    (r'\btodo!\s*\(\s*\)', 'todo!() macro'),
    (r'\bpanic!\s*\(\s*"not implemented', 'panic not implemented'),
    (r'raise\s+NotImplementedError\s*\(\s*\)', 'bare NotImplementedError'),
    (r'#\s*implement\s*(later|this|here)', 'implement later comment'),
    (r'//\s*implement\s*(later|this|here)', 'implement later comment'),
    (r'def\s+\w+\s*\([^)]*\)\s*:\s*(pass|\.\.\.)\s*$', 'empty function'),
    (r'fn\s+\w+\s*\([^)]*\)\s*\{\s*\}', 'empty function body'),
    (r'return\s+None\s*#.*stub', 'stub return'),
]

COMPILED_PATTERNS = [(re.compile(p, re.IGNORECASE | re.MULTILINE), desc) for p, desc in STUB_PATTERNS]


def check_for_stubs(file_path):
    """Check file for stub patterns. Returns list of (line_num, pattern_desc, line_content)."""
    if not os.path.exists(file_path):
        return []

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            lines = content.split('\n')
    except (OSError, Exception):
        return []

    findings = []
    for line_num, line in enumerate(lines, 1):
        for pattern, desc in COMPILED_PATTERNS:
            if pattern.search(line):
                if 'NotImplementedError' in line and re.search(r'NotImplementedError\s*\(\s*["\'][^"\']+["\']', line):
                    continue
                findings.append((line_num, desc, line.strip()[:60]))

    return findings


def find_project_root(file_path, marker_files):
    """Walk up from file_path looking for project root markers."""
    current = os.path.dirname(os.path.abspath(file_path))
    for _ in range(10):  # Max 10 levels up
        for marker in marker_files:
            if os.path.exists(os.path.join(current, marker)):
                return current
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return None


def run_linter(file_path, max_errors=10):
    """Run appropriate linter and return first N errors."""
    ext = os.path.splitext(file_path)[1].lower()
    errors = []

    try:
        if ext == '.rs':
            # Rust: run cargo clippy from project root
            project_root = find_project_root(file_path, ['Cargo.toml'])
            if project_root:
                result = subprocess.run(
                    ['cargo', 'clippy', '--message-format=short', '--quiet'],
                    cwd=project_root,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if result.stderr:
                    for line in result.stderr.split('\n'):
                        if line.strip() and ('error' in line.lower() or 'warning' in line.lower()):
                            errors.append(line.strip()[:100])
                            if len(errors) >= max_errors:
                                break

        elif ext == '.py':
            # Python: try flake8, fall back to py_compile
            try:
                result = subprocess.run(
                    ['flake8', '--max-line-length=120', file_path],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                for line in result.stdout.split('\n'):
                    if line.strip():
                        errors.append(line.strip()[:100])
                        if len(errors) >= max_errors:
                            break
            except FileNotFoundError:
                # flake8 not installed, try py_compile
                result = subprocess.run(
                    ['python', '-m', 'py_compile', file_path],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.stderr:
                    errors.append(result.stderr.strip()[:200])

        elif ext in ('.js', '.ts', '.tsx', '.jsx'):
            # JavaScript/TypeScript: try eslint
            project_root = find_project_root(file_path, ['package.json', '.eslintrc', '.eslintrc.js', '.eslintrc.json'])
            if project_root:
                try:
                    result = subprocess.run(
                        ['npx', 'eslint', '--format=compact', file_path],
                        cwd=project_root,
                        capture_output=True,
                        text=True,
                        timeout=30
                    )
                    for line in result.stdout.split('\n'):
                        if line.strip() and (':' in line):
                            errors.append(line.strip()[:100])
                            if len(errors) >= max_errors:
                                break
                except FileNotFoundError:
                    pass

        elif ext == '.go':
            # Go: run go vet
            project_root = find_project_root(file_path, ['go.mod'])
            if project_root:
                result = subprocess.run(
                    ['go', 'vet', './...'],
                    cwd=project_root,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if result.stderr:
                    for line in result.stderr.split('\n'):
                        if line.strip():
                            errors.append(line.strip()[:100])
                            if len(errors) >= max_errors:
                                break

    except subprocess.TimeoutExpired:
        errors.append("(linter timed out)")
    except (OSError, Exception) as e:
        pass  # Linter not available, skip silently

    return errors


def is_test_file(file_path):
    """Check if file is a test file."""
    basename = os.path.basename(file_path).lower()
    dirname = os.path.dirname(file_path).lower()

    # Common test file patterns
    test_patterns = [
        'test_', '_test.', '.test.', 'spec.', '_spec.',
        'tests.', 'testing.', 'mock.', '_mock.'
    ]
    # Common test directories
    test_dirs = ['test', 'tests', '__tests__', 'spec', 'specs', 'testing']

    for pattern in test_patterns:
        if pattern in basename:
            return True

    for test_dir in test_dirs:
        if test_dir in dirname.split(os.sep):
            return True

    return False


def find_test_files(file_path, project_root):
    """Find test files related to source file."""
    if not project_root:
        return []

    ext = os.path.splitext(file_path)[1]
    basename = os.path.basename(file_path)
    name_without_ext = os.path.splitext(basename)[0]

    # Patterns to look for
    test_patterns = []

    if ext == '.rs':
        # Rust: look for mod tests in same file, or tests/ directory
        test_patterns = [
            os.path.join(project_root, 'tests', '**', f'*{name_without_ext}*'),
            os.path.join(project_root, '**', 'tests', f'*{name_without_ext}*'),
        ]
    elif ext == '.py':
        test_patterns = [
            os.path.join(project_root, '**', f'test_{name_without_ext}.py'),
            os.path.join(project_root, '**', f'{name_without_ext}_test.py'),
            os.path.join(project_root, 'tests', '**', f'*{name_without_ext}*.py'),
        ]
    elif ext in ('.js', '.ts', '.tsx', '.jsx'):
        base = name_without_ext.replace('.test', '').replace('.spec', '')
        test_patterns = [
            os.path.join(project_root, '**', f'{base}.test{ext}'),
            os.path.join(project_root, '**', f'{base}.spec{ext}'),
            os.path.join(project_root, '**', '__tests__', f'{base}*'),
        ]
    elif ext == '.go':
        test_patterns = [
            os.path.join(os.path.dirname(file_path), f'{name_without_ext}_test.go'),
        ]

    found = []
    for pattern in test_patterns:
        found.extend(glob.glob(pattern, recursive=True))

    return list(set(found))[:5]  # Limit to 5


def get_test_reminder(file_path, project_root):
    """Check if tests should be run and return reminder message."""
    if is_test_file(file_path):
        return None  # Editing a test file, no reminder needed

    ext = os.path.splitext(file_path)[1]
    code_extensions = ('.rs', '.py', '.js', '.ts', '.tsx', '.jsx', '.go')

    if ext not in code_extensions:
        return None

    # Check for marker file
    marker_dir = project_root or os.path.dirname(file_path)
    marker_file = os.path.join(marker_dir, '.chainlink', 'last_test_run')

    code_modified_after_tests = False

    if os.path.exists(marker_file):
        try:
            marker_mtime = os.path.getmtime(marker_file)
            file_mtime = os.path.getmtime(file_path)
            code_modified_after_tests = file_mtime > marker_mtime
        except OSError:
            code_modified_after_tests = True
    else:
        # No marker = tests haven't been run
        code_modified_after_tests = True

    if not code_modified_after_tests:
        return None

    # Find test files
    test_files = find_test_files(file_path, project_root)

    # Generate test command based on project type
    test_cmd = None
    if ext == '.rs' and project_root:
        if os.path.exists(os.path.join(project_root, 'Cargo.toml')):
            test_cmd = 'cargo test'
    elif ext == '.py':
        if project_root and os.path.exists(os.path.join(project_root, 'pytest.ini')):
            test_cmd = 'pytest'
        elif project_root and os.path.exists(os.path.join(project_root, 'setup.py')):
            test_cmd = 'python -m pytest'
    elif ext in ('.js', '.ts', '.tsx', '.jsx') and project_root:
        if os.path.exists(os.path.join(project_root, 'package.json')):
            test_cmd = 'npm test'
    elif ext == '.go' and project_root:
        test_cmd = 'go test ./...'

    if test_files or test_cmd:
        msg = "🧪 TEST REMINDER: Code modified since last test run."
        if test_cmd:
            msg += f"\n   Run: {test_cmd}"
        if test_files:
            msg += f"\n   Related tests: {', '.join(os.path.basename(t) for t in test_files[:3])}"
        return msg

    return None


def main():
    try:
        input_data = json.load(sys.stdin)
    except (json.JSONDecodeError, Exception):
        sys.exit(0)

    tool_name = input_data.get("tool_name", "")
    tool_input = input_data.get("tool_input", {})

    if tool_name not in ("Write", "Edit"):
        sys.exit(0)

    file_path = tool_input.get("file_path", "")

    code_extensions = (
        '.rs', '.py', '.js', '.ts', '.tsx', '.jsx', '.go', '.java',
        '.c', '.cpp', '.h', '.hpp', '.cs', '.rb', '.php', '.swift',
        '.kt', '.scala', '.zig', '.odin'
    )

    if not any(file_path.endswith(ext) for ext in code_extensions):
        sys.exit(0)

    if '.claude' in file_path and 'hooks' in file_path:
        sys.exit(0)

    # Find project root for linter and test detection
    project_root = find_project_root(file_path, [
        'Cargo.toml', 'package.json', 'go.mod', 'setup.py',
        'pyproject.toml', '.git'
    ])

    # Check for stubs
    stub_findings = check_for_stubs(file_path)

    # Run linter
    linter_errors = run_linter(file_path)

    # Check for test reminder
    test_reminder = get_test_reminder(file_path, project_root)

    # Build output
    messages = []

    if stub_findings:
        stub_list = "\n".join([f"  Line {ln}: {desc} - `{content}`" for ln, desc, content in stub_findings[:5]])
        if len(stub_findings) > 5:
            stub_list += f"\n  ... and {len(stub_findings) - 5} more"
        messages.append(f"""⚠️ STUB PATTERNS DETECTED in {file_path}:
{stub_list}

Fix these NOW - replace with real implementation.""")

    if linter_errors:
        error_list = "\n".join([f"  {e}" for e in linter_errors[:10]])
        if len(linter_errors) > 10:
            error_list += f"\n  ... and more"
        messages.append(f"""🔍 LINTER ISSUES:
{error_list}""")

    if test_reminder:
        messages.append(test_reminder)

    if messages:
        output = {
            "hookSpecificOutput": {
                "hookEventName": "PostToolUse",
                "additionalContext": "\n\n".join(messages)
            }
        }
    else:
        output = {
            "hookSpecificOutput": {
                "hookEventName": "PostToolUse",
                "additionalContext": f"✓ {os.path.basename(file_path)} - no issues detected"
            }
        }

    print(json.dumps(output))
    sys.exit(0)


if __name__ == "__main__":
    main()
