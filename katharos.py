#!/usr/bin/env python3
"""
Katharos — Ancient Greek καθαρός (clean, pure)

A development cruft cleanup tool that scans directories for regenerable
build artifacts (node_modules, __pycache__, .venv, etc.) and provides
an interactive per-item cleanup workflow.

Designed to reclaim disk space from development backup folders before
deduplication workflows like monosis.

Usage:
    katharos <path>                    # Scan and interactively clean
    katharos <path> --dry-run          # Just report findings
    katharos <path> --min-size 10M     # Only show items > 10 MiB
    katharos --show-keep               # Show persistent keep list
    katharos --reset-keep              # Clear persistent keep list
"""

import argparse
import fnmatch
import os
import shutil
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

import tomllib

try:
    import termios
    import tty

    _HAS_TERMIOS = True
except ImportError:
    _HAS_TERMIOS = False
from typing import Optional

from rich import box
from rich.table import Table

from auxiliary import format_bytes, format_path_for_display
from console_ui import ConsoleUI
from kosmos_config import SharedConfigManager

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


class RuleType(Enum):
    FOLDER = "folder"
    FILE = "file"


class Certainty(Enum):
    HIGH = "high"
    MEDIUM = "medium"


@dataclass
class CruftRule:
    pattern: str
    category: str
    rule_type: RuleType
    certainty: Certainty = Certainty.HIGH
    requires_project_context: bool = False
    description: str = ""


@dataclass
class CruftFinding:
    path: str
    rule: CruftRule
    size: int = 0
    item_count: int = 0


@dataclass
class ScanResult:
    root_path: str
    findings: list[CruftFinding] = field(default_factory=list)
    scan_duration: float = 0.0
    total_size: int = 0
    category_summary: dict[str, dict] = field(default_factory=dict)


class Decision(Enum):
    DELETE = "delete"
    SKIP = "skip"
    KEEP = "keep"


@dataclass
class ReviewResult:
    """Collected decisions (before execution) and execution outcomes."""

    # Decisions collected during review
    decisions: dict[str, Decision] = field(default_factory=dict)
    findings_by_path: dict[str, CruftFinding] = field(default_factory=dict)
    ignore_patterns: list[str] = field(default_factory=list)

    # Execution outcomes (filled after confirm)
    deleted: list[str] = field(default_factory=list)
    total_reclaimed: int = 0
    errors: list[tuple[str, str]] = field(default_factory=list)

    @property
    def to_delete(self) -> list[CruftFinding]:
        return [self.findings_by_path[p] for p, d in self.decisions.items() if d is Decision.DELETE]

    @property
    def to_keep(self) -> list[str]:
        return [p for p, d in self.decisions.items() if d is Decision.KEEP]

    @property
    def skipped(self) -> list[str]:
        return [p for p, d in self.decisions.items() if d is Decision.SKIP]

    @property
    def delete_size(self) -> int:
        return sum(f.size for f in self.to_delete)


# ---------------------------------------------------------------------------
# Rules loading from TOML
# ---------------------------------------------------------------------------

RULES_FILE = Path(__file__).parent / "katharos_rules.toml"

_TYPE_MAP = {"folder": RuleType.FOLDER, "file": RuleType.FILE}
_CERTAINTY_MAP = {"high": Certainty.HIGH, "medium": Certainty.MEDIUM}


def load_rules(path: Path = RULES_FILE) -> tuple[list[CruftRule], list[str], list[str]]:
    """Load cruft rules and project-context lists from a TOML file.

    Returns (rules, project_context_files, project_context_globs).
    """
    with path.open("rb") as f:
        data = tomllib.load(f)

    ctx = data.get("project_context", {})
    ctx_files: list[str] = ctx.get("files", [])
    ctx_globs: list[str] = ctx.get("globs", [])

    rules: list[CruftRule] = []
    for entry in data.get("rules", []):
        rules.append(
            CruftRule(
                pattern=entry["pattern"],
                category=entry.get("category", entry["pattern"]),
                rule_type=_TYPE_MAP[entry["type"]],
                certainty=_CERTAINTY_MAP.get(entry.get("certainty", "high"), Certainty.HIGH),
                requires_project_context=entry.get("requires_project_context", False),
                description=entry.get("description", ""),
            )
        )

    return rules, ctx_files, ctx_globs


# Load once at import time
CRUFT_RULES, PROJECT_CONTEXT_FILES, PROJECT_CONTEXT_GLOBS = load_rules()


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _has_project_context(directory: str) -> bool:
    """Check whether a directory looks like a project root."""
    try:
        entries = list(Path(directory).iterdir())
    except OSError:
        return False
    for entry in entries:
        name = entry.name if isinstance(entry, Path) else entry
        if name in PROJECT_CONTEXT_FILES:
            return True
        for glob_pat in PROJECT_CONTEXT_GLOBS:
            if fnmatch.fnmatch(name, glob_pat):
                return True
    return False


def _match_folder_rule(name: str, parent: str, rule: CruftRule) -> bool:
    """Return True if *name* matches a folder rule, considering project context."""
    if rule.rule_type is not RuleType.FOLDER:
        return False
    # fnmatch handles both exact and glob patterns (e.g. *.egg-info)
    if not fnmatch.fnmatch(name, rule.pattern):
        return False
    return not (rule.requires_project_context and not _has_project_context(parent))


def _match_file_rule(name: str, rule: CruftRule) -> bool:
    """Return True if *name* matches a file rule."""
    if rule.rule_type is not RuleType.FILE:
        return False
    return fnmatch.fnmatch(name, rule.pattern)


def _dir_size(path: str) -> tuple[int, int]:
    """Return (total_bytes, file_count) for a directory tree."""
    total = 0
    count = 0
    try:
        for dirpath, _dirnames, filenames in os.walk(path, followlinks=False):
            for f in filenames:
                try:
                    total += os.lstat(Path(dirpath) / f).st_size
                    count += 1
                except OSError:
                    pass
    except OSError:
        pass
    return total, count


def _parse_size(value: str) -> int:
    """Parse a human-readable size string like '10M' into bytes."""
    value = value.strip().upper()
    multipliers = {"B": 1, "K": 1024, "M": 1024**2, "G": 1024**3}
    for suffix, mult in multipliers.items():
        if value.endswith(suffix):
            return int(float(value[: -len(suffix)]) * mult)
    return int(value)


def _get_single_key() -> str:
    """Read a single keypress without requiring Enter.

    Falls back to input() if the terminal doesn't support raw mode.
    """
    if not _HAS_TERMIOS:
        return input("> ").strip()[:1]
    try:
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return ch
    except (termios.error, OSError):
        return input("> ").strip()[:1]


# ---------------------------------------------------------------------------
# Katharos
# ---------------------------------------------------------------------------


class Katharos:
    """Main application class for the Katharos cruft cleanup tool."""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.ui = ConsoleUI()
        self._shutdown_requested = False

        self.config_manager = SharedConfigManager()
        self._config = self.config_manager.load()
        self._katharos_cfg: dict = self._config.get_tool_config("katharos") or {
            "ignore_paths": [],
            "ignore_patterns": [],
            "last_run": None,
            "stats": {"total_runs": 0, "total_reclaimed_bytes": 0},
        }
        self._ignore_set: set[str] = set(self._katharos_cfg.get("ignore_paths", []))

        signal.signal(signal.SIGINT, self._signal_handler)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, self._signal_handler)

    # -- signal handling ----------------------------------------------------

    def _signal_handler(self, signum, frame):
        if self._shutdown_requested:
            sys.exit(1)
        self._shutdown_requested = True
        self.ui.print_warning("\nShutdown requested... press Ctrl+C again to force quit.")

    # -- config persistence -------------------------------------------------

    def _save_config(self):
        self._katharos_cfg["ignore_paths"] = sorted(self._ignore_set)
        self._config.set_tool_config("katharos", self._katharos_cfg)
        self.config_manager.save(self._config)

    def _record_run(self, reclaimed: int):
        stats = self._katharos_cfg.setdefault("stats", {"total_runs": 0, "total_reclaimed_bytes": 0})
        stats["total_runs"] = stats.get("total_runs", 0) + 1
        stats["total_reclaimed_bytes"] = stats.get("total_reclaimed_bytes", 0) + reclaimed
        self._katharos_cfg["last_run"] = datetime.now(timezone.utc).isoformat()
        self._save_config()

    # -- keep list commands --------------------------------------------------

    def show_keep(self):
        paths = sorted(self._ignore_set)
        patterns = self._katharos_cfg.get("ignore_patterns", [])
        if not paths and not patterns:
            self.ui.print_info("Keep list is empty.")
            return
        if paths:
            self.ui.print_info("Kept paths (always skipped):")
            for p in paths:
                self.ui.print_plain(f"  {format_path_for_display(p)}")
        if patterns:
            self.ui.print_info("Kept patterns (always skipped):")
            for p in patterns:
                self.ui.print_plain(f"  {p}")

    def reset_keep(self):
        self._ignore_set.clear()
        self._katharos_cfg["ignore_patterns"] = []
        self._save_config()
        self.ui.print_success("Keep list cleared.")

    # -- scanning ------------------------------------------------------------

    def scan(self, root: str, min_size: int = 0) -> ScanResult:
        root = str(Path(root).resolve())
        result = ScanResult(root_path=root)
        start = time.monotonic()

        folder_rules = [r for r in CRUFT_RULES if r.rule_type is RuleType.FOLDER]
        file_rules = [r for r in CRUFT_RULES if r.rule_type is RuleType.FILE]

        self.ui.print_header("Katharos", f"Scanning {format_path_for_display(root)}")

        progress = self.ui.create_activity_progress()
        with progress:
            task = progress.add_task("Scanning...", total=None)
            dirs_scanned = 0

            for dirpath, dirs, files in os.walk(root, topdown=True, followlinks=False):
                if self._shutdown_requested:
                    break

                dirs_scanned += 1
                if dirs_scanned % 200 == 0:
                    progress.update(task, description=f"Scanning... {dirs_scanned} dirs")

                # Check folders against rules (and prune matches)
                matched_dirs: set[str] = set()
                for d in list(dirs):
                    full = str(Path(dirpath) / d)
                    if full in self._ignore_set:
                        matched_dirs.add(d)
                        continue
                    for rule in folder_rules:
                        if _match_folder_rule(d, dirpath, rule):
                            size, count = _dir_size(full)
                            if size >= min_size:
                                result.findings.append(CruftFinding(full, rule, size, count))
                            matched_dirs.add(d)
                            break

                # Prune matched dirs so we don't descend into them
                dirs[:] = [d for d in dirs if d not in matched_dirs]

                # Check files against rules
                for f in files:
                    full = str(Path(dirpath) / f)
                    if full in self._ignore_set:
                        continue
                    for rule in file_rules:
                        if _match_file_rule(f, rule):
                            try:
                                size = os.lstat(full).st_size
                            except OSError:
                                size = 0
                            if size >= min_size:
                                result.findings.append(CruftFinding(full, rule, size, 1))
                            break

            progress.update(task, description=f"Scan complete — {dirs_scanned} dirs")

        result.scan_duration = time.monotonic() - start
        result.total_size = sum(f.size for f in result.findings)

        # Build category summary
        cats: dict[str, dict] = {}
        for f in result.findings:
            cat = f.rule.category
            entry = cats.setdefault(cat, {"count": 0, "size": 0, "files": 0, "type": f.rule.rule_type})
            entry["count"] += 1
            entry["size"] += f.size
            entry["files"] += f.item_count
        result.category_summary = cats

        return result

    # -- reporting -----------------------------------------------------------

    def report(self, result: ScanResult):
        if not result.findings:
            self.ui.print_success("No cruft found!")
            return

        table = Table(title="Cruft Summary", box=box.ROUNDED, show_lines=False)
        table.add_column("Category", style="cyan", min_width=20)
        table.add_column("Type", style="dim", justify="center", min_width=6)
        table.add_column("Items", justify="right", min_width=6)
        table.add_column("Files", justify="right", style="dim", min_width=8)
        table.add_column("Size", justify="right", style="yellow", min_width=10)

        # Sort by size descending
        total_files = 0
        sorted_cats = sorted(result.category_summary.items(), key=lambda x: x[1]["size"], reverse=True)
        for cat, info in sorted_cats:
            is_folder = info["type"] is RuleType.FOLDER
            rtype = "folder" if is_folder else "file"
            files_col = f"{info['files']:,}" if is_folder else ""
            table.add_row(cat, rtype, str(info["count"]), files_col, format_bytes(info["size"]))
            total_files += info["files"]

        self.ui.console.print(table)
        self.ui.console.print()
        files_note = f", {total_files:,} files inside" if total_files else ""
        self.ui.print_info(
            f"Total reclaimable: {format_bytes(result.total_size)}  "
            f"({len(result.findings)} items in {len(result.category_summary)} categories{files_note})"
        )
        self.ui.print_info(f"Scan completed in {result.scan_duration:.1f}s")

    # -- interactive review (collect decisions only) --------------------------

    def review(self, result: ScanResult) -> ReviewResult:
        review = ReviewResult()
        if not result.findings:
            return review

        # Index findings by path for later execution
        for f in result.findings:
            review.findings_by_path[f.path] = f

        # Group findings by category, sorted by total size desc
        by_cat: dict[str, list[CruftFinding]] = {}
        for f in result.findings:
            by_cat.setdefault(f.rule.category, []).append(f)

        cat_sizes = {cat: sum(f.size for f in items) for cat, items in by_cat.items()}
        sorted_cats = sorted(by_cat.keys(), key=lambda c: cat_sizes[c], reverse=True)

        marked_delete = 0

        for cat in sorted_cats:
            if self._shutdown_requested:
                break

            items = by_cat[cat]
            cat_size = cat_sizes[cat]
            rule_type = items[0].rule.rule_type

            self.ui.console.print()
            self.ui.console.print(
                f"[bold]── {cat} ── {len(items)} {'folders' if rule_type is RuleType.FOLDER else 'files'}"
                f", {format_bytes(cat_size)} ──[/bold]"
            )
            desc = items[0].rule.description
            if desc:
                self.ui.console.print(f"  [dim italic]{desc}[/dim italic]")

            if rule_type is RuleType.FOLDER:
                marked_delete = self._review_folders(items, review, marked_delete)
            else:
                marked_delete = self._review_files(items, cat, review, marked_delete)

        return review

    def _mark(
        self, review: ReviewResult, finding: CruftFinding, decision: Decision, marked_delete: int, quiet: bool = False
    ) -> int:
        """Record a decision for a finding. Returns updated marked_delete count."""
        review.decisions[finding.path] = decision
        if decision is Decision.DELETE:
            marked_delete += finding.size
            if not quiet:
                self.ui.print_info(f"  Marked for deletion — {format_bytes(marked_delete)} total queued")
        elif decision is Decision.KEEP:
            if not quiet:
                self.ui.print_info("  Marked to keep")
        return marked_delete

    def _review_folders(self, items: list[CruftFinding], review: ReviewResult, marked_delete: int) -> int:
        items.sort(key=lambda f: f.size, reverse=True)
        for idx, finding in enumerate(items):
            if self._shutdown_requested:
                break

            display = format_path_for_display(finding.path)
            self.ui.console.print(
                f"\n  [dim][{idx + 1}/{len(items)}][/dim] {display}"
                f" [yellow]({format_bytes(finding.size)}, {finding.item_count:,} files)[/yellow]"
            )

            while True:
                self.ui.console.print(
                    "  [dim]\\[d]elete  \\[s]kip  \\[l]ist  \\[k]eep"
                    "  \\[S]kip category  \\[D]elete rest  \\[q]uit[/dim] ",
                    end="",
                )

                key = _get_single_key()
                self.ui.console.print()

                if key == "l":
                    self._list_folder(finding.path)
                    continue
                break

            if key == "d":
                marked_delete = self._mark(review, finding, Decision.DELETE, marked_delete)
            elif key == "s":
                review.decisions[finding.path] = Decision.SKIP
            elif key == "k":
                marked_delete = self._mark(review, finding, Decision.KEEP, marked_delete)
            elif key == "S":
                for remaining in items[idx:]:
                    review.decisions[remaining.path] = Decision.SKIP
                break
            elif key == "D":
                self.ui.console.print(
                    f"  Mark remaining {len(items) - idx} folders for deletion? [dim]\\[y]es  \\[n]o[/dim] ", end=""
                )
                confirm = _get_single_key()
                self.ui.console.print()
                if confirm in ("y", "Y"):
                    count = len(items) - idx
                    for remaining in items[idx:]:
                        marked_delete = self._mark(review, remaining, Decision.DELETE, marked_delete, quiet=True)
                    self.ui.print_info(
                        f"  Marked {count} folders for deletion — {format_bytes(marked_delete)} total queued"
                    )
                else:
                    review.decisions[finding.path] = Decision.SKIP
                break
            elif key in ("q", "\x03"):
                self._shutdown_requested = True
                break
            else:
                review.decisions[finding.path] = Decision.SKIP

        return marked_delete

    def _list_folder(self, folder_path: str, max_entries: int = 20):
        """Show top-level contents of a folder."""
        try:
            entries = sorted(Path(folder_path).iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
        except OSError as e:
            self.ui.print_error(f"  Cannot list: {e}")
            return

        for i, entry in enumerate(entries):
            if i >= max_entries:
                self.ui.console.print(f"  [dim]  ... and {len(entries) - max_entries} more[/dim]")
                break
            if entry.is_dir():
                self.ui.console.print(f"  [dim]  {entry.name}/[/dim]")
            else:
                try:
                    size = entry.lstat().st_size
                except OSError:
                    size = 0
                self.ui.console.print(f"  [dim]  {entry.name} ({format_bytes(size)})[/dim]")

    def _review_files(self, items: list[CruftFinding], category: str, review: ReviewResult, marked_delete: int) -> int:
        self.ui.console.print(
            "\n  [dim]\\[d]elete all  \\[s]kip all  \\[r]eview individually  \\[k]eep pattern  \\[q]uit[/dim] ",
            end="",
        )

        key = _get_single_key()
        self.ui.console.print()

        if key == "d":
            for f in items:
                marked_delete = self._mark(review, f, Decision.DELETE, marked_delete, quiet=True)
            self.ui.print_info(f"  Marked {len(items)} files for deletion — {format_bytes(marked_delete)} total queued")
        elif key == "s":
            for f in items:
                review.decisions[f.path] = Decision.SKIP
        elif key == "r":
            marked_delete = self._review_files_individually(items, review, marked_delete)
        elif key == "k":
            pattern = items[0].rule.pattern
            review.ignore_patterns.append(pattern)
            for f in items:
                review.decisions[f.path] = Decision.KEEP
            self.ui.print_info(f"  Pattern '{pattern}' will be added to keep list")
        elif key in ("q", "\x03"):
            self._shutdown_requested = True
        else:
            for f in items:
                review.decisions[f.path] = Decision.SKIP

        return marked_delete

    def _review_files_individually(self, items: list[CruftFinding], review: ReviewResult, marked_delete: int) -> int:
        items.sort(key=lambda f: f.size, reverse=True)
        for idx, finding in enumerate(items):
            if self._shutdown_requested:
                break
            display = format_path_for_display(finding.path)
            self.ui.console.print(
                f"\n  [dim][{idx + 1}/{len(items)}][/dim] {display} [yellow]({format_bytes(finding.size)})[/yellow]"
            )
            self.ui.console.print(
                "  [dim]\\[d]elete  \\[s]kip  \\[k]eep  \\[S]kip rest  \\[D]elete rest  \\[q]uit[/dim] ", end=""
            )

            key = _get_single_key()
            self.ui.console.print()

            if key == "d":
                marked_delete = self._mark(review, finding, Decision.DELETE, marked_delete)
            elif key == "s":
                review.decisions[finding.path] = Decision.SKIP
            elif key == "k":
                marked_delete = self._mark(review, finding, Decision.KEEP, marked_delete)
            elif key == "S":
                for remaining in items[idx:]:
                    review.decisions[remaining.path] = Decision.SKIP
                break
            elif key == "D":
                count = len(items) - idx
                for remaining in items[idx:]:
                    marked_delete = self._mark(review, remaining, Decision.DELETE, marked_delete, quiet=True)
                self.ui.print_info(f"  Marked {count} files for deletion — {format_bytes(marked_delete)} total queued")
                break
            elif key in ("q", "\x03"):
                self._shutdown_requested = True
                break
            else:
                review.decisions[finding.path] = Decision.SKIP

        return marked_delete

    # -- decision summary & execution ----------------------------------------

    def show_decisions(self, review: ReviewResult):
        """Show a summary of decisions and ask for confirmation."""
        to_del = review.to_delete
        to_keep = review.to_keep
        skipped = review.skipped

        # Count folders vs files and total underlying files
        del_folders = [f for f in to_del if f.rule.rule_type is RuleType.FOLDER]
        del_files = [f for f in to_del if f.rule.rule_type is RuleType.FILE]
        del_inner_files = sum(f.item_count for f in del_folders)

        self.ui.console.print()
        self.ui.print_info("Decision Summary")
        if to_del:
            parts = []
            if del_folders:
                parts.append(f"{len(del_folders)} folders ({del_inner_files:,} files inside)")
            if del_files:
                parts.append(f"{len(del_files)} files")
            self.ui.print_warning(f"  Delete:  {', '.join(parts)} — {format_bytes(review.delete_size)}")
        if skipped:
            self.ui.print_plain(f"  Skip:    {len(skipped)} items")
        if to_keep:
            self.ui.print_info(f"  Keep:    {len(to_keep)} paths (added to keep list)")
        if review.ignore_patterns:
            self.ui.print_info(f"  Keep patterns: {', '.join(review.ignore_patterns)}")

        if not to_del and not to_keep and not review.ignore_patterns:
            self.ui.print_info("  Nothing to do.")
            return False

        self.ui.console.print()
        return self.ui.confirm("Execute these changes?", default=True)

    def execute(self, review: ReviewResult):
        """Execute collected decisions: delete items, save keep list."""
        # Apply keep paths
        for path in review.to_keep:
            self._ignore_set.add(path)

        # Apply keep patterns
        for pattern in review.ignore_patterns:
            self._katharos_cfg.setdefault("ignore_patterns", []).append(pattern)

        if review.to_keep or review.ignore_patterns:
            self._save_config()

        # Delete items
        to_del = review.to_delete
        if not to_del:
            self._record_run(0)
            return

        progress = self.ui.create_progress()
        with progress:
            task = progress.add_task("Deleting...", total=len(to_del))
            for finding in to_del:
                if self._shutdown_requested:
                    break
                progress.update(
                    task,
                    description=f"Deleting {finding.rule.category}... {format_bytes(review.total_reclaimed)} reclaimed",
                )
                try:
                    if finding.rule.rule_type is RuleType.FOLDER:
                        shutil.rmtree(finding.path)
                    else:
                        Path(finding.path).unlink()
                    review.deleted.append(finding.path)
                    review.total_reclaimed += finding.size
                except OSError as e:
                    review.errors.append((finding.path, str(e)))
                progress.advance(task)

    def summary(self, review: ReviewResult):
        """Show final execution results."""
        self.ui.console.print()
        self.ui.print_info("Cleanup Complete")

        if review.deleted:
            self.ui.print_success(
                f"  Deleted: {len(review.deleted)} items, reclaimed {format_bytes(review.total_reclaimed)}"
            )
        if review.to_keep:
            self.ui.print_info(f"  Kept:    {len(review.to_keep)} paths added to keep list")
        if review.ignore_patterns:
            self.ui.print_info(f"  Kept:    {len(review.ignore_patterns)} patterns added to keep list")
        if review.errors:
            self.ui.print_error(f"  Errors:  {len(review.errors)}")
            for path, err in review.errors:
                self.ui.print_error(f"    {format_path_for_display(path)}: {err}")

        self._record_run(review.total_reclaimed)

    # -- main entry point ----------------------------------------------------

    def run(self):
        # Handle ignore-list commands (no path required)
        if getattr(self.args, "show_keep", False):
            self.show_keep()
            return
        if getattr(self.args, "reset_keep", False):
            self.reset_keep()
            return

        path = getattr(self.args, "path", None)
        if not path:
            self.ui.print_error("Please provide a path to scan.")
            sys.exit(1)

        if not Path(path).is_dir():
            self.ui.print_error(f"Not a directory: {path}")
            sys.exit(1)

        min_size = 0
        if getattr(self.args, "min_size", None):
            min_size = _parse_size(self.args.min_size)

        result = self.scan(path, min_size)
        self.report(result)

        if getattr(self.args, "dry_run", False):
            return

        if not result.findings:
            return

        self.ui.console.print()
        if not self.ui.confirm("Proceed with interactive review?", default=True):
            return

        review_result = self.review(result)

        if self.show_decisions(review_result):
            self.execute(review_result)
            self.summary(review_result)
        else:
            self.ui.print_info("No changes made.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="katharos",
        description="Katharos — development cruft cleanup tool",
    )
    parser.add_argument("path", nargs="?", help="Directory to scan")
    parser.add_argument("--dry-run", action="store_true", help="Report findings without interactive cleanup")
    parser.add_argument("--min-size", type=str, default=None, help="Minimum item size to report (e.g. 10M, 1G)")
    parser.add_argument("--show-keep", action="store_true", help="Show persistent keep list")
    parser.add_argument("--reset-keep", action="store_true", help="Clear persistent keep list")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    app = Katharos(args)
    app.run()


if __name__ == "__main__":
    main()
