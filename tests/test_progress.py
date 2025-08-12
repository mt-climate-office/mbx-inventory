"""Tests for progress reporting functionality."""

import logging
import time
from io import StringIO

import pytest
from rich.console import Console

from mbx_inventory.cli.progress import ProgressReporter


class TestProgressReporter:
    """Test cases for ProgressReporter class."""

    def test_init_default(self):
        """Test default initialization."""
        reporter = ProgressReporter()

        assert reporter.verbose is False
        assert reporter.console is not None
        assert reporter.progress is None
        assert reporter.current_task is None
        assert reporter.operation_start_time is None
        assert reporter.errors == []
        assert reporter.logger is not None

    def test_init_verbose(self):
        """Test initialization with verbose mode."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(verbose=True, console=console)

        assert reporter.verbose is True
        assert reporter.console is console
        assert reporter.logger.level == logging.DEBUG

    def test_start_operation_with_total(self):
        """Test starting operation with total items."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(console=console)

        reporter.start_operation("Test Operation", total_items=100)

        assert reporter.progress is not None
        assert reporter.current_task is not None
        assert reporter.operation_start_time is not None
        assert reporter.errors == []

        # Clean up
        reporter.progress.stop()

    def test_start_operation_indeterminate(self):
        """Test starting operation without total items."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(console=console)

        reporter.start_operation("Test Operation")

        assert reporter.progress is not None
        assert reporter.current_task is not None
        assert reporter.operation_start_time is not None

        # Clean up
        reporter.progress.stop()

    def test_update_progress(self):
        """Test updating progress."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(console=console)

        reporter.start_operation("Test Operation", total_items=100)

        # Test increment update
        reporter.update_progress(10, "Processing items")

        # Test with message only
        reporter.update_progress(message="Still processing")

        reporter.complete_operation("Test completed")

    def test_set_progress(self):
        """Test setting absolute progress."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(console=console)

        reporter.start_operation("Test Operation", total_items=100)

        reporter.set_progress(50, "Half way done")
        reporter.set_progress(100, "Complete")

        reporter.complete_operation("Test completed")

    def test_complete_operation(self):
        """Test completing an operation."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(console=console)

        reporter.start_operation("Test Operation", total_items=100)

        # Add some delay to test duration calculation
        time.sleep(0.01)

        reporter.complete_operation("Operation finished successfully")

        assert reporter.progress is None
        assert reporter.current_task is None
        assert reporter.operation_start_time is None

    def test_report_error(self):
        """Test error reporting."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(console=console)

        context = {"table": "test_table", "record_id": "123"}
        reporter.report_error("Test error message", context)

        assert len(reporter.errors) == 1
        assert reporter.errors[0]["message"] == "Test error message"
        assert reporter.errors[0]["context"] == context
        assert "timestamp" in reporter.errors[0]

    def test_report_warning(self):
        """Test warning reporting."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(verbose=True, console=console)

        context = {"field": "test_field", "value": "test_value"}

        # Should not raise any exceptions
        reporter.report_warning("Test warning message", context)

    def test_log_info(self):
        """Test info logging."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(console=console)

        context = {"operation": "sync", "table": "elements"}

        # Should not raise any exceptions
        reporter.log_info("Test info message", context)

    def test_log_debug_verbose(self):
        """Test debug logging in verbose mode."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(verbose=True, console=console)

        context = {"detail": "test_detail"}

        # Should not raise any exceptions
        reporter.log_debug("Test debug message", context)

    def test_log_debug_non_verbose(self):
        """Test debug logging in non-verbose mode."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(verbose=False, console=console)

        # Should not raise any exceptions (but won't log anything)
        reporter.log_debug("Test debug message")

    def test_display_error_summary_no_errors(self):
        """Test displaying error summary with no errors."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(console=console)

        # Should not raise any exceptions
        reporter.display_error_summary()

    def test_display_error_summary_with_errors(self):
        """Test displaying error summary with errors."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(console=console)

        # Add some errors
        reporter.report_error("Error 1", {"context": "value1"})
        reporter.report_error("Error 2", {"context": "value2"})

        # Should not raise any exceptions
        reporter.display_error_summary()

    def test_display_summary_table_empty(self):
        """Test displaying empty summary table."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(console=console)

        # Should not raise any exceptions
        reporter.display_summary_table("Test Table", [])

    def test_display_summary_table_with_data(self):
        """Test displaying summary table with data."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(console=console)

        data = [
            {"table_name": "elements", "records_created": 10, "records_updated": 5},
            {"table_name": "stations", "records_created": 3, "records_updated": 2},
        ]

        # Should not raise any exceptions
        reporter.display_summary_table("Sync Results", data)

    def test_operation_context_success(self):
        """Test operation context manager with successful operation."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(console=console)

        with reporter.operation_context("Test Operation", 10) as ctx:
            assert ctx is reporter
            assert reporter.progress is not None
            ctx.update_progress(5, "Half done")

        # Context should clean up automatically
        assert reporter.progress is None

    def test_operation_context_with_exception(self):
        """Test operation context manager with exception."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(console=console)

        with pytest.raises(ValueError):
            with reporter.operation_context("Test Operation", 10):
                raise ValueError("Test error")

        # Context should clean up even after exception
        assert reporter.progress is None
        assert len(reporter.errors) == 1
        assert "Operation failed" in reporter.errors[0]["message"]

    def test_logging_setup(self):
        """Test logging setup."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(verbose=True, console=console)

        # Check logger configuration
        assert reporter.logger.name == "mbx_inventory"
        assert reporter.logger.level == logging.DEBUG
        assert len(reporter.logger.handlers) == 1
        assert reporter.logger.propagate is False

    def test_multiple_operations(self):
        """Test handling multiple sequential operations."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(console=console)

        # First operation
        reporter.start_operation("Operation 1", 50)
        reporter.update_progress(25)
        reporter.complete_operation("Operation 1 done")

        # Second operation
        reporter.start_operation("Operation 2", 30)
        reporter.update_progress(15)
        reporter.complete_operation("Operation 2 done")

        # Should handle multiple operations without issues
        assert reporter.progress is None
        assert reporter.current_task is None
