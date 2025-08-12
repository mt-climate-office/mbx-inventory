"""Tests for logging integration throughout the CLI system."""

import json
import logging
import tempfile
from io import StringIO
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from rich.console import Console

from mbx_inventory.cli.config import InventoryConfig
from mbx_inventory.cli.progress import ProgressReporter


class TestLoggingIntegration:
    """Test cases for logging integration."""

    def test_progress_reporter_logging_setup(self):
        """Test that progress reporter sets up logging correctly."""
        console = Console(file=StringIO(), width=80)

        # Test non-verbose mode
        reporter = ProgressReporter(verbose=False, console=console)
        assert reporter.logger.level == logging.INFO
        assert len(reporter.logger.handlers) == 1
        assert reporter.logger.propagate is False

        # Test verbose mode
        reporter_verbose = ProgressReporter(verbose=True, console=console)
        assert reporter_verbose.logger.level == logging.DEBUG

    def test_progress_reporter_structured_logging(self):
        """Test structured logging with context."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(verbose=True, console=console)

        # Test logging with context
        context = {"table": "elements", "records": 100}

        # These should not raise exceptions
        reporter.log_info("Test info message", context)
        reporter.log_debug("Test debug message", context)
        reporter.report_warning("Test warning", context)
        reporter.report_error("Test error", context)

    def test_config_loading_with_logging(self):
        """Test configuration loading with logging integration."""
        # Create a temporary config file
        config_data = {
            "backend": {
                "type": "airtable",
                "config": {"api_key": "test_key", "base_id": "test_base"},
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass",
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            config_path = Path(f.name)

        try:
            # Mock the logger to capture log messages
            with patch("mbx_inventory.cli.config.logging.getLogger") as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger

                # Load configuration
                config = InventoryConfig.load_from_file(config_path)

                # Verify logging calls were made
                mock_logger.debug.assert_called()
                mock_logger.info.assert_called_with("Configuration loaded successfully")

                assert config.backend.type == "airtable"

        finally:
            config_path.unlink()

    def test_config_loading_error_logging(self):
        """Test error logging during configuration loading."""
        # Test with non-existent file
        non_existent_path = Path("non_existent_config.json")

        with patch("mbx_inventory.cli.config.logging.getLogger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger

            with pytest.raises(FileNotFoundError):
                InventoryConfig.load_from_file(non_existent_path)

            # Verify error was logged
            mock_logger.error.assert_called()

    def test_config_environment_validation_logging(self):
        """Test logging during environment variable validation."""
        config_data = {
            "backend": {
                "type": "airtable",
                "config": {"api_key": "${MISSING_VAR}", "base_id": "test_base"},
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass",
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            config_path = Path(f.name)

        try:
            with patch("mbx_inventory.cli.config.logging.getLogger") as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger

                # This should fail due to missing environment variable
                with pytest.raises(ValueError):
                    InventoryConfig.load_from_file(config_path)

                # Verify error was logged
                mock_logger.error.assert_called()

        finally:
            config_path.unlink()

    @pytest.mark.asyncio
    async def test_database_connectivity_logging(self):
        """Test logging during database connectivity validation."""
        config_data = {
            "backend": {
                "type": "airtable",
                "config": {"api_key": "test_key", "base_id": "test_base"},
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass",
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            config_path = Path(f.name)

        try:
            config = InventoryConfig.load_from_file(config_path)

            with patch("mbx_inventory.cli.config.logging.getLogger") as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger

                # Mock the database connection to fail
                with patch(
                    "mbx_inventory.cli.config.create_async_engine"
                ) as mock_engine:
                    mock_engine.side_effect = Exception("Connection failed")

                    result = await config.validate_database_connectivity()

                    assert result is False
                    mock_logger.debug.assert_called()
                    mock_logger.error.assert_called()

        finally:
            config_path.unlink()

    def test_backend_instance_creation_logging(self):
        """Test logging during backend instance creation."""
        config_data = {
            "backend": {
                "type": "airtable",
                "config": {"api_key": "test_key", "base_id": "test_base"},
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass",
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            config_path = Path(f.name)

        try:
            config = InventoryConfig.load_from_file(config_path)

            with patch("mbx_inventory.cli.config.logging.getLogger") as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger

                # Mock the backend classes
                with patch("mbx_inventory.cli.config.AirtableBackend") as mock_backend:
                    mock_instance = Mock()
                    mock_backend.return_value = mock_instance

                    backend = config.get_backend_instance()

                    assert backend is mock_instance
                    mock_logger.debug.assert_called_with(
                        "Creating airtable backend instance"
                    )

        finally:
            config_path.unlink()

    def test_inventory_instance_creation_logging(self):
        """Test logging during inventory instance creation."""
        config_data = {
            "backend": {
                "type": "airtable",
                "config": {"api_key": "test_key", "base_id": "test_base"},
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass",
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            config_path = Path(f.name)

        try:
            config = InventoryConfig.load_from_file(config_path)

            with patch("mbx_inventory.cli.config.logging.getLogger") as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger

                # Mock the backend and inventory classes
                with patch("mbx_inventory.cli.config.AirtableBackend") as mock_backend:
                    with patch("mbx_inventory.cli.config.Inventory") as mock_inventory:
                        mock_backend_instance = Mock()
                        mock_backend.return_value = mock_backend_instance
                        mock_inventory_instance = Mock()
                        mock_inventory.return_value = mock_inventory_instance

                        inventory = config.get_inventory_instance()

                        assert inventory is mock_inventory_instance
                        mock_logger.debug.assert_any_call("Creating inventory instance")
                        mock_logger.debug.assert_any_call(
                            "Inventory instance created successfully"
                        )

        finally:
            config_path.unlink()

    def test_error_context_preservation(self):
        """Test that error context is preserved in logging."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(verbose=True, console=console)

        # Test error reporting with context
        context = {
            "operation": "sync",
            "table": "elements",
            "record_id": "rec123",
            "error_code": "VALIDATION_FAILED",
        }

        reporter.report_error("Record validation failed", context)

        # Verify error was stored with context
        assert len(reporter.errors) == 1
        error_info = reporter.errors[0]
        assert error_info["message"] == "Record validation failed"
        assert error_info["context"] == context
        assert "timestamp" in error_info

    def test_summary_reporting(self):
        """Test summary reporting functionality."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(console=console)

        # Test summary table display
        summary_data = [
            {
                "table_name": "elements",
                "records_created": 10,
                "records_updated": 5,
                "records_failed": 1,
            },
            {
                "table_name": "stations",
                "records_created": 3,
                "records_updated": 2,
                "records_failed": 0,
            },
        ]

        # Should not raise exceptions
        reporter.display_summary_table("Sync Results", summary_data)

        # Test with empty data
        reporter.display_summary_table("Empty Results", [])

    def test_operation_context_logging(self):
        """Test logging within operation context."""
        console = Console(file=StringIO(), width=80)
        reporter = ProgressReporter(verbose=True, console=console)

        # Test successful operation
        with reporter.operation_context("Test Operation", 10) as ctx:
            ctx.log_info("Starting processing")
            ctx.update_progress(5, "Half done")
            ctx.log_debug("Debug information")

        # Test operation with error
        with pytest.raises(ValueError):
            with reporter.operation_context("Failing Operation", 5):
                reporter.log_info("This will fail")
                raise ValueError("Test error")

        # Verify error was captured
        assert len(reporter.errors) == 1
        assert "Operation failed" in reporter.errors[0]["message"]
