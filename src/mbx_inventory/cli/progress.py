"""Progress reporting and logging functionality for inventory CLI operations."""

import logging
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table


class ProgressReporter:
    """Progress reporter with rich console output for inventory operations."""

    def __init__(self, verbose: bool = False, console: Optional[Console] = None):
        """Initialize the progress reporter.

        Args:
            verbose: Enable verbose logging mode
            console: Rich console instance (creates new one if None)
        """
        self.verbose = verbose
        self.console = console or Console()
        self.progress: Optional[Progress] = None
        self.current_task: Optional[TaskID] = None
        self.operation_start_time: Optional[float] = None
        self.errors: List[Dict[str, Any]] = []

        # Set up logging
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Set up structured logging with rich handler."""
        # Create logger
        self.logger = logging.getLogger("mbx_inventory")
        self.logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)

        # Remove existing handlers to avoid duplicates
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        # Add rich handler
        rich_handler = RichHandler(
            console=self.console,
            show_time=True,
            show_path=self.verbose,
            rich_tracebacks=True,
        )
        rich_handler.setLevel(logging.DEBUG if self.verbose else logging.INFO)

        # Set formatter
        formatter = logging.Formatter("%(message)s", datefmt="[%X]")
        rich_handler.setFormatter(formatter)

        self.logger.addHandler(rich_handler)

        # Prevent propagation to root logger
        self.logger.propagate = False

    def start_operation(
        self, operation: str, total_items: Optional[int] = None
    ) -> None:
        """Start a new operation with progress tracking.

        Args:
            operation: Description of the operation
            total_items: Total number of items to process (None for indeterminate)
        """
        self.operation_start_time = time.time()
        self.errors.clear()

        # Create progress bar
        columns = [
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
        ]

        if total_items is not None:
            columns.extend(
                [
                    BarColumn(),
                    MofNCompleteColumn(),
                    TimeElapsedColumn(),
                    TimeRemainingColumn(),
                ]
            )
        else:
            columns.append(TimeElapsedColumn())

        self.progress = Progress(*columns, console=self.console)
        self.progress.start()

        self.current_task = self.progress.add_task(operation, total=total_items)

        self.logger.info(f"Starting {operation}")
        if self.verbose and total_items is not None:
            self.logger.debug(f"Processing {total_items} items")

    def update_progress(self, completed: int = 1, message: str = "") -> None:
        """Update progress for the current operation.

        Args:
            completed: Number of items completed (increment)
            message: Optional status message
        """
        if self.progress and self.current_task is not None:
            self.progress.update(self.current_task, advance=completed)

            if message:
                self.progress.update(self.current_task, description=message)
                if self.verbose:
                    self.logger.debug(message)

    def set_progress(self, completed: int, message: str = "") -> None:
        """Set absolute progress for the current operation.

        Args:
            completed: Total number of items completed
            message: Optional status message
        """
        if self.progress and self.current_task is not None:
            self.progress.update(self.current_task, completed=completed)

            if message:
                self.progress.update(self.current_task, description=message)
                if self.verbose:
                    self.logger.debug(message)

    def complete_operation(self, summary: str) -> None:
        """Complete the current operation and display summary.

        Args:
            summary: Summary message for the completed operation
        """
        if self.progress:
            self.progress.stop()
            self.progress = None
            self.current_task = None

        duration = 0.0
        if self.operation_start_time:
            duration = time.time() - self.operation_start_time
            self.operation_start_time = None

        self.logger.info(f"Completed: {summary}")
        if self.verbose:
            self.logger.debug(f"Operation took {duration:.2f} seconds")

        # Display error summary if there were errors
        if self.errors:
            self.display_error_summary()

    def report_error(
        self, error: str, context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Report an error with optional context.

        Args:
            error: Error message
            context: Additional context information
        """
        error_info = {
            "message": error,
            "context": context or {},
            "timestamp": time.time(),
        }
        self.errors.append(error_info)

        self.logger.error(error)
        if self.verbose and context:
            for key, value in context.items():
                self.logger.debug(f"  {key}: {value}")

    def report_warning(
        self, warning: str, context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Report a warning with optional context.

        Args:
            warning: Warning message
            context: Additional context information
        """
        self.logger.warning(warning)
        if self.verbose and context:
            for key, value in context.items():
                self.logger.debug(f"  {key}: {value}")

    def log_info(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        """Log an informational message.

        Args:
            message: Info message
            context: Additional context information
        """
        self.logger.info(message)
        if self.verbose and context:
            for key, value in context.items():
                self.logger.debug(f"  {key}: {value}")

    def log_debug(self, message: str, context: Optional[Dict[str, Any]] = None) -> None:
        """Log a debug message (only shown in verbose mode).

        Args:
            message: Debug message
            context: Additional context information
        """
        if self.verbose:
            self.logger.debug(message)
            if context:
                for key, value in context.items():
                    self.logger.debug(f"  {key}: {value}")

    def display_error_summary(self) -> None:
        """Display a summary of all errors encountered."""
        if not self.errors:
            return

        self.console.print("\n[bold red]Error Summary:[/bold red]")

        table = Table(show_header=True, header_style="bold red")
        table.add_column("Error", style="red")
        table.add_column("Context", style="dim")

        for error_info in self.errors:
            context_str = ""
            if error_info["context"]:
                context_items = [f"{k}: {v}" for k, v in error_info["context"].items()]
                context_str = ", ".join(context_items)

            table.add_row(error_info["message"], context_str)

        self.console.print(table)
        self.console.print(f"\nTotal errors: {len(self.errors)}")

    def display_summary_table(self, title: str, data: List[Dict[str, Any]]) -> None:
        """Display a summary table with the given data.

        Args:
            title: Table title
            data: List of dictionaries containing table data
        """
        if not data:
            return

        self.console.print(f"\n[bold]{title}:[/bold]")

        # Create table with columns based on first row keys
        table = Table(show_header=True, header_style="bold blue")

        if data:
            for key in data[0].keys():
                table.add_column(key.replace("_", " ").title())

            for row in data:
                table.add_row(*[str(value) for value in row.values()])

        self.console.print(table)

    @contextmanager
    def operation_context(self, operation: str, total_items: Optional[int] = None):
        """Context manager for operations with automatic cleanup.

        Args:
            operation: Description of the operation
            total_items: Total number of items to process
        """
        self.start_operation(operation, total_items)
        try:
            yield self
        except Exception as e:
            self.report_error(f"Operation failed: {str(e)}")
            raise
        finally:
            if self.progress:
                self.progress.stop()
                self.progress = None
                self.current_task = None
