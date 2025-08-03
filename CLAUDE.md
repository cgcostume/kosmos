# Claude Code Instructions

This document provides MANDATORY instructions for Claude Code when working on this project.

## CRITICAL: Automatic Code Formatting & Linting

### YOU MUST ALWAYS FORMAT AND LINT PYTHON CODE

**After EVERY Python file creation or modification, you MUST:**

```bash
ruff check <filename> --fix
ruff format <filename>
```

**DO NOT** proceed with testing or further work until these steps are complete.

### Why This Matters
- Saves tokens by catching errors early
- Ensures consistent code style
- Reduces back-and-forth iterations
- Makes the code more maintainable

### Quick Validation Without Running
Instead of running Python files to check for errors, use:
```bash
ruff check <filename>
```
This is MUCH faster and saves tokens!

## Project-Specific Guidelines

1. **Python Version**: Target Python 3.9+ (3.12+ is installed)
2. **Code Style**: 
   - Line length: 120 characters
   - Use double quotes for strings
   - Follow Google docstring convention
3. **Imports**: Let Ruff handle import sorting with isort rules
4. **Type Hints**: Use type hints for function signatures where it improves clarity

## Common Commands

### Before committing any Python changes:
```bash
# Fix all auto-fixable issues and format
ruff check . --fix && ruff format .
```

### To check a specific file without running it:
```bash
# Just check for issues (no fixes)
ruff check path/to/file.py

# Check and show what would be fixed
ruff check path/to/file.py --fix --diff
```

### To see all issues in detail:
```bash
ruff check . --show-source
```

## Token-Saving Tips

1. Use `ruff check` instead of running Python files to validate syntax
2. Use `ruff format --check` to see if formatting is needed before applying
3. Run Ruff on specific files rather than the entire project when possible
4. Use `--fix` to automatically resolve simple issues

## Workflow Optimization

When modifying Python files:
1. Make your changes
2. Run `ruff check <file> --fix` to fix and validate
3. Run `ruff format <file>` to ensure consistent formatting
4. Only then test the actual functionality

This reduces back-and-forth iterations and saves tokens for both of us!