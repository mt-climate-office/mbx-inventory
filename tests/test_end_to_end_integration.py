"""End-to-end integration tests for mbx-inventory CLI."""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch
from typer.testing import CliRunner

from mbx_inventory.cli.main import app
from mbx_inventory.cli.config import InventoryConfig
from mbx_inventory.cli.progress import ProgressReporter


class TestEndToEndIntegration:
    """Test complete end-to-end workflows."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    def create_test_config_file(self, config_data: dict) -> Path:
        """Create a temporary configuration file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            return Path(f.name)

    def test_complete_config_workflow(self):
        """Test complete configuration workflow from file to validation."""
        # Create valid configuration
        config_data = {
            "backend": {
                "type": "airtable",
                "config": {"api_key": "test_api_key", "base_id": "test_base_id"},
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_password",
            },
            "table_mappings": {"elements": "Elements", "stations": "Stations"},
            "sync_options": {"batch_size": 50, "timeout": 60, "retry_attempts": 2},
        }

        config_file = self.create_test_config_file(config_data)

        try:
            # Test config show command
            result = self.runner.invoke(
                app, ["config", "show", "--config", str(config_file)]
            )
            assert result.exit_code == 0
            assert "airtable" in result.stdout
            assert "localhost" in result.stdout
            assert "Elements" in result.stdout

            # Test config validate command
            result = self.runner.invoke(
                app, ["config", "validate", "--config", str(config_file), "--verbose"]
            )
            assert result.exit_code == 0
            assert "Configuration file is valid" in result.stdout

        finally:
            config_file.unlink()

    def test_sync_dry_run_workflow(self):
        """Test complete sync dry-run workflow."""
        config_data = {
            "backend": {
                "type": "airtable",
                "config": {"api_key": "test_api_key", "base_id": "test_base_id"},
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_password",
            },
            "table_mappings": {"elements": "Elements"},
        }

        config_file = self.create_test_config_file(config_data)

        try:
            with patch(
                "mbx_inventory.cli.config.InventoryConfig.get_inventory_instance"
            ) as mock_get_inventory:
                with patch(
                    "mbx_inventory.cli.sync_engine.create_async_engine"
                ) as mock_create_engine:
                    with patch(
                        "mbx_inventory.cli.sync_engine.SyncEngine.sync_all_tables"
                    ) as mock_sync:
                        # Mock inventory instance
                        mock_inventory = Mock()
                        mock_get_inventory.return_value = mock_inventory

                        # Mock database engine
                        mock_engine = AsyncMock()
                        mock_create_engine.return_value = mock_engine

                        # Mock sync result
                        from mbx_inventory.cli.sync_engine import (
                            SyncSummary,
                            TableSyncResult,
                        )

                        mock_sync_result = SyncSummary(
                            total_tables=1,
                            successful_tables=1,
                            failed_tables=0,
                            total_records_processed=10,
                            total_records_created=5,
                            total_records_updated=5,
                            total_duration_seconds=2.5,
                            table_results=[
                                TableSyncResult(
                                    table_name="elements",
                                    records_created=5,
                                    records_updated=5,
                                    records_failed=0,
                                    errors=[],
                                    duration_seconds=2.5,
                                )
                            ],
                            errors=[],
                        )
                        mock_sync.return_value = mock_sync_result

                        # Test dry-run sync
                        result = self.runner.invoke(
                            app,
                            [
                                "sync",
                                "--config",
                                str(config_file),
                                "--dry-run",
                                "--verbose",
                            ],
                        )

                        assert result.exit_code == 0
                        assert "dry-run mode" in result.stdout.lower()
                        mock_sync.assert_called_once()
                        call_args = mock_sync.call_args
                        assert call_args[1]["dry_run"] is True

        finally:
            config_file.unlink()

    def test_selective_table_sync_workflow(self):
        """Test sync workflow with table filtering."""
        config_data = {
            "backend": {
                "type": "airtable",
                "config": {"api_key": "test_api_key", "base_id": "test_base_id"},
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_password",
            },
            "table_mappings": {"elements": "Elements", "stations": "Stations"},
        }

        config_file = self.create_test_config_file(config_data)

        try:
            with patch(
                "mbx_inventory.cli.config.InventoryConfig.get_inventory_instance"
            ) as mock_get_inventory:
                with patch(
                    "mbx_inventory.cli.sync_engine.create_async_engine"
                ) as mock_create_engine:
                    with patch(
                        "mbx_inventory.cli.sync_engine.SyncEngine.sync_all_tables"
                    ) as mock_sync:
                        # Mock inventory instance
                        mock_inventory = Mock()
                        mock_get_inventory.return_value = mock_inventory

                        # Mock database engine
                        mock_engine = AsyncMock()
                        mock_create_engine.return_value = mock_engine

                        # Mock sync result
                        from mbx_inventory.cli.sync_engine import (
                            SyncSummary,
                            TableSyncResult,
                        )

                        mock_sync_result = SyncSummary(
                            total_tables=1,
                            successful_tables=1,
                            failed_tables=0,
                            total_records_processed=5,
                            total_records_created=3,
                            total_records_updated=2,
                            total_duration_seconds=1.5,
                            table_results=[
                                TableSyncResult(
                                    table_name="elements",
                                    records_created=3,
                                    records_updated=2,
                                    records_failed=0,
                                    errors=[],
                                    duration_seconds=1.5,
                                )
                            ],
                            errors=[],
                        )
                        mock_sync.return_value = mock_sync_result

                        # Test selective table sync
                        result = self.runner.invoke(
                            app,
                            [
                                "sync",
                                "--config",
                                str(config_file),
                                "--tables",
                                "elements",
                                "--verbose",
                            ],
                        )

                        assert result.exit_code == 0
                        mock_sync.assert_called_once()
                        call_args = mock_sync.call_args
                        assert call_args[1]["table_filter"] == ["elements"]

        finally:
            config_file.unlink()

    def test_error_handling_workflow(self):
        """Test error handling in complete workflow."""
        # Test with invalid configuration
        invalid_config = {"backend": {"type": "invalid_backend"}}

        config_file = self.create_test_config_file(invalid_config)

        try:
            # Test config validation with invalid backend
            result = self.runner.invoke(
                app, ["config", "validate", "--config", str(config_file)]
            )
            assert result.exit_code == 1
            assert "Error" in result.stdout

            # Test sync with invalid config
            result = self.runner.invoke(app, ["sync", "--config", str(config_file)])
            assert result.exit_code == 1
            assert "Error" in result.stdout

        finally:
            config_file.unlink()

    def test_help_system_integration(self):
        """Test that help system works for all commands."""
        # Test main help
        result = self.runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "Inventory synchronization CLI" in result.stdout
        assert "validate" in result.stdout
        assert "sync" in result.stdout
        assert "config" in result.stdout

        # Test validate help
        result = self.runner.invoke(app, ["validate", "--help"])
        assert result.exit_code == 0
        assert "Validate backend connection" in result.stdout

        # Test sync help
        result = self.runner.invoke(app, ["sync", "--help"])
        assert result.exit_code == 0
        assert "Sync inventory data" in result.stdout
        assert "--dry-run" in result.stdout
        assert "--tables" in result.stdout

        # Test config help
        result = self.runner.invoke(app, ["config", "--help"])
        assert result.exit_code == 0
        assert "Configuration management" in result.stdout

        # Test config subcommand help
        result = self.runner.invoke(app, ["config", "show", "--help"])
        assert result.exit_code == 0
        assert "Display current configuration" in result.stdout

        result = self.runner.invoke(app, ["config", "validate", "--help"])
        assert result.exit_code == 0
        assert "Validate configuration file" in result.stdout

    def test_configuration_loading_integration(self):
        """Test configuration loading with various scenarios."""
        # Test with environment variable substitution
        config_data = {
            "backend": {
                "type": "airtable",
                "config": {"api_key": "${TEST_API_KEY}", "base_id": "${TEST_BASE_ID}"},
            },
            "database": {
                "host": "${TEST_DB_HOST}",
                "port": 5432,
                "database": "${TEST_DB_NAME}",
                "username": "${TEST_DB_USER}",
                "password": "${TEST_DB_PASS}",
            },
        }

        config_file = self.create_test_config_file(config_data)

        try:
            # Set environment variables
            import os

            os.environ["TEST_API_KEY"] = "test_key"
            os.environ["TEST_BASE_ID"] = "test_base"
            os.environ["TEST_DB_HOST"] = "localhost"
            os.environ["TEST_DB_NAME"] = "test_db"
            os.environ["TEST_DB_USER"] = "test_user"
            os.environ["TEST_DB_PASS"] = "test_pass"

            # Test config loading
            config = InventoryConfig.load_from_file(config_file)
            assert config.backend.config["api_key"] == "test_key"
            assert config.backend.config["base_id"] == "test_base"
            assert config.database.host == "localhost"
            assert config.database.database == "test_db"

            # Clean up environment variables
            for var in [
                "TEST_API_KEY",
                "TEST_BASE_ID",
                "TEST_DB_HOST",
                "TEST_DB_NAME",
                "TEST_DB_USER",
                "TEST_DB_PASS",
            ]:
                if var in os.environ:
                    del os.environ[var]

        finally:
            config_file.unlink()

    def test_progress_reporting_integration(self):
        """Test progress reporting throughout the workflow."""
        from rich.console import Console
        from io import StringIO

        # Create a console that captures output
        output = StringIO()
        console = Console(file=output, width=80)
        reporter = ProgressReporter(verbose=True, console=console)

        # Test progress reporting functionality
        with reporter.operation_context("Test Operation", total_items=3):
            reporter.update_progress(1, "Step 1 completed")
            reporter.update_progress(1, "Step 2 completed")
            reporter.update_progress(1, "Step 3 completed")
            reporter.complete_operation("All steps completed")

        output_str = output.getvalue()
        assert "Test Operation" in output_str
        assert "Step 1 completed" in output_str
        assert "All steps completed" in output_str

    def test_cli_consistency_with_mbx_db(self):
        """Test that CLI follows similar patterns to mbx-db."""
        # Test that command structure is consistent
        result = self.runner.invoke(app, ["--help"])
        assert result.exit_code == 0

        # Should have similar command patterns
        assert "validate" in result.stdout  # Similar to mbx-db validate
        assert "config" in result.stdout  # Similar to mbx-db config commands

        # Test that error handling is consistent
        result = self.runner.invoke(app, ["validate", "--config", "nonexistent.json"])
        assert result.exit_code == 1

        # Test that verbose flag works consistently
        config_data = {
            "backend": {
                "type": "airtable",
                "config": {"api_key": "test", "base_id": "test"},
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test",
                "username": "test",
                "password": "test",
            },
        }
        config_file = self.create_test_config_file(config_data)

        try:
            result = self.runner.invoke(
                app, ["config", "validate", "--config", str(config_file), "--verbose"]
            )
            assert result.exit_code == 0
            # Verbose output should contain debug information
            assert any(level in result.stdout for level in ["DEBUG", "INFO"])
        finally:
            config_file.unlink()
