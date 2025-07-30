#!/usr/bin/env python3
"""
Generic Console UI Module using Rich

Provides a comprehensive console interface with colors, progress bars, tables,
panels, and interactive prompts. Designed to be reusable across different CLI applications.
"""

from typing import List, Dict, Any, Optional, Callable
from rich.console import Console
from rich.progress import Progress, TaskID, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.text import Text
from rich.tree import Tree
from rich import box
from rich.columns import Columns
import sys


class ConsoleUI:
    """Generic console UI handler using Rich for beautiful CLI interfaces"""
    
    def __init__(self, force_terminal: bool = None):
        """Initialize console with optional terminal forcing"""
        self.console = Console(force_terminal=force_terminal, highlight=False)
        self._current_progress: Optional[Progress] = None
        self._progress_tasks: Dict[str, TaskID] = {}
    
    # Basic styled output methods
    def print_success(self, message: str):
        """Print success message in green"""
        self.console.print(message, style="green")
    
    def print_error(self, message: str):
        """Print error message in red"""
        self.console.print(message, style="red bold")
    
    def print_warning(self, message: str):
        """Print warning message in yellow"""
        self.console.print(message, style="yellow")
    
    def print_info(self, message: str):
        """Print info message in cyan"""
        self.console.print(message, style="cyan")
    
    def print_progress(self, message: str):
        """Print progress message in dim white"""
        self.console.print(message, style="white dim")
    
    def print_header(self, title: str, subtitle: str = None):
        """Print a header with optional subtitle"""
        if subtitle:
            header_text = f"[bold]{title}[/bold]\n[dim]{subtitle}[/dim]"
        else:
            header_text = f"[bold]{title}[/bold]"
        
        panel = Panel(header_text, box=box.ROUNDED, padding=(0, 1))
        self.console.print(panel)
    
    # Configuration display
    def show_configuration(self, config: Dict[str, Any], title: str = "Configuration"):
        """Display configuration in a formatted table"""
        # Create table without title
        table = Table(show_header=False, box=box.SIMPLE)
        table.add_column("Setting", style="cyan dim", min_width=20, justify="right")
        table.add_column("Value", style="cyan", min_width=30)
        
        for key, value in config.items():
            if isinstance(value, list):
                value = ", ".join(str(v) for v in value)
            table.add_row(key, str(value))
        
        self.console.print(table)
    
    # Progress bar management
    def create_progress(self):
        """Create a Rich progress context manager for batch operations"""
        from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn, TimeRemainingColumn
        
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self.console
        )
    
    def create_progress_bar(self, description: str = "Processing") -> str:
        """Create a new progress bar and return its ID"""
        if not self._current_progress:
            self._current_progress = Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("({task.completed}/{task.total})"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=self.console
            )
            self._current_progress.start()
        
        task_id = self._current_progress.add_task(description, total=100)
        progress_key = f"progress_{len(self._progress_tasks)}"
        self._progress_tasks[progress_key] = task_id
        return progress_key
    
    def update_progress(self, progress_key: str, completed: int, total: int = None):
        """Update progress bar"""
        if self._current_progress and progress_key in self._progress_tasks:
            task_id = self._progress_tasks[progress_key]
            if total:
                self._current_progress.update(task_id, total=total, completed=completed)
            else:
                self._current_progress.update(task_id, completed=completed)
    
    def finish_progress(self):
        """Finish and clean up all progress bars"""
        if self._current_progress:
            self._current_progress.stop()
            self._current_progress = None
            self._progress_tasks.clear()
    
    # File operation displays
    def show_file_operations_preview(self, operations: Dict[str, Any], title: str = "Operations Preview"):
        """Show a preview of file operations in a table"""
        if not operations:
            self.print_info("No operations to preview")
            return
        
        table = Table(title=title, show_lines=True)
        table.add_column("Source", style="white dim", min_width=30)
        table.add_column("→", justify="center", width=3)
        table.add_column("Target", style="white", min_width=30)
        
        for source, target in operations.items():
            # Handle both string paths and objects with attributes
            if hasattr(target, 'name'):
                target_name = target.name
            else:
                target_name = str(target)
            
            if hasattr(source, 'name'):
                source_name = source.name  
            else:
                source_name = str(source)
            
            table.add_row(source_name, "→", target_name)
        
        self.console.print(table)
        self.console.print()
    
    def show_grouped_files(self, grouped_files: Dict[str, List[Any]], 
                          title: str = "Files by Category",
                          show_count: bool = True):
        """Show files grouped by category (e.g., duplicates, issues, etc.)"""
        if not grouped_files:
            self.print_info(f"No files found for {title.lower()}")
            return
        
        for category, files in grouped_files.items():
            if not files:
                continue
            
            category_title = f"{category}"
            if show_count:
                category_title += f" ({len(files)} files)"
            
            self.console.print(f"\n[yellow]{category_title}:[/yellow]")
            
            # Show first few files, then "and X more" if too many
            show_limit = 5
            for i, file_item in enumerate(files[:show_limit]):
                if hasattr(file_item, 'name'):
                    filename = file_item.name
                elif isinstance(file_item, tuple) and len(file_item) >= 2:
                    filename = f"{file_item[0]} ({file_item[1]})"  # filename and error
                else:
                    filename = str(file_item)
                
                self.console.print(f"[white dim]    • {filename}[/white dim]")
            
            if len(files) > show_limit:
                remaining = len(files) - show_limit
                self.console.print(f"[white dim]    • ... and {remaining} more[/white dim]")
    
    def show_issues_report(self, issues: Dict[str, List[str]], title: str = "Issues Found"):
        """Show issues grouped by type"""
        if not issues:
            self.print_info("No issues found")
            return
        
        self.console.print(f"\n[red]{title}:[/red]")
        
        for issue_type, filenames in issues.items():
            if not filenames:
                continue
            
            self.console.print(f"[red]  {issue_type} ({len(filenames)} files):[/red]")
            
            # Show first few filenames
            show_limit = 3
            for filename in filenames[:show_limit]:
                self.console.print(f"[red dim]    • {filename}[/red dim]")
            
            if len(filenames) > show_limit:
                remaining = len(filenames) - show_limit
                self.console.print(f"[red dim]    • ... and {remaining} more[/red dim]")
    
    def show_operation_summary(self, successful: List[str], failed: List[tuple], 
                             operation_name: str = "operation"):
        """Show summary of completed operations"""
        if successful:
            self.print_success(f"Successfully {operation_name} {len(successful)} files")
        
        if failed:
            self.print_error(f"Failed to {operation_name} {len(failed)} files:")
            for filename, error in failed:
                self.console.print(f"[red dim]  • {filename}: {error}[/red dim]")
    
    # Interactive prompts
    def confirm(self, question: str, default: bool = False) -> bool:
        """Ask for yes/no confirmation"""
        return Confirm.ask(question, default=default, console=self.console)
    
    def prompt(self, question: str, default: str = None, choices: List[str] = None) -> str:
        """Ask for text input with optional default and choices"""
        return Prompt.ask(question, default=default, choices=choices, console=self.console)
    
    def select_from_list(self, items: List[str], title: str = "Select items") -> List[str]:
        """Allow user to select multiple items from a list"""
        if not items:
            return []
        
        self.console.print(f"\n[cyan]{title}:[/cyan]")
        for i, item in enumerate(items, 1):
            self.console.print(f"  {i}. {item}")
        
        while True:
            response = self.prompt(
                "Enter numbers separated by commas (e.g., 1,3,5) or 'all' for all items",
                default="all"
            )
            
            if response.lower() == 'all':
                return items.copy()
            
            try:
                indices = [int(x.strip()) - 1 for x in response.split(',')]
                selected = [items[i] for i in indices if 0 <= i < len(items)]
                return selected
            except (ValueError, IndexError):
                self.print_error("Invalid selection. Please try again.")
    
    # Utility methods
    def clear_screen(self):
        """Clear the console screen"""
        self.console.clear()
    
    def print_separator(self, char: str = "─", length: int = 50):
        """Print a separator line"""
        self.console.print(char * length, style="dim")
    
    def pause(self, message: str = "Press Enter to continue..."):
        """Pause execution until Enter is pressed"""
        input(message)