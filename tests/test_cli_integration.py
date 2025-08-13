"""
Integration tests for CLI commands.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from typer.testing import CliRunner

from mbx_inventory.cli.main import app


class TestCLIIntegration:
    """Integration tests for CLI commands."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

        # Sample configuration data
        self.config_data = {
            "backend": {
                "type": "airtable",
                "config": {"api_key": "test_api_key", "base_id": "test_base_id"},
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass",
            },
            "sync_options": {"batch_size": 50, "timeout": 30, "retry_attempts": 3},
        }

    def create_test_config_file(self, config_data):
        """Create a temporary config file for testing."""
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump(config_data, temp_file, indent=2)
        temp_file.close()
        return Path(temp_file.name)

    def test_validate_command_integration(self):
        """Test validate command integration."""
        config_file = self.create_test_config_file(self.config_data)

        try:
            with patch(
                "mbx_inventory.cli.config.AirtableBackend"
            ) as mock_backend_class:
                with patch("asyncio.run") as mock_asyncio_run:
                    # Mock backend validation
                    mock_backend = MagicMock()
                    mock_backend.validate.return_value = True
                    mock_backend_class.return_value = mock_backend

                    # Mock database validation
                    mock_asyncio_run.return_value = True

                    result = self.runner.invoke(
                        app, ["validate", "--config", str(config_file), "--verbose"]
                    )

                    assert result.exit_code == 0
                    assert "Configuration loaded successfully" in result.stdout
                    assert "Backend connection successful" in result.stdout
                    assert "Database connection successful" in result.stdout

        finally:
            config_file.unlink()

    def test_config_show_command_integration(self):
        """Test config show command integration."""
        config_file = self.create_test_config_file(self.config_data)

        try:
            result = self.runner.invoke(
                app, ["config", "show", "--config", str(config_file)]
            )

            assert result.exit_code == 0
            assert "Backend Configuration" in result.stdout
            assert "airtable" in result.stdout
            assert "Database Configuration" in result.stdout
            assert "localhost" in result.stdout

        finally:
            config_file.unlink()

    def test_config_validate_command_integration(self):
        """Test config validate command integration."""
        config_file = self.create_test_config_file(self.config_data)

        try:
            result = self.runner.invoke(
                app, ["config", "validate", "--config", str(config_file), "--verbose"]
            )

            assert result.exit_code == 0
            assert "Configuration file is valid" in result.stdout
            assert "Backend: airtable" in result.stdout

        finally:
            config_file.unlink()

    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    @patch("mbx_inventory.cli.config.InventoryConfig.get_inventory_instance")
    @patch("mbx_inventory.cli.sync_engine.SyncEngine")
    def test_sync_command_dry_run_integration(
        self, mock_sync_engine_class, mock_get_inventory, mock_create_engine
    ):
        """Test sync command with dry-run integration."""
        config_file = self.create_test_config_file(self.config_data)

        try:
            # Mock inventory instance
            mock_inventory = MagicMock()
            mock_get_inventory.return_value = mock_inventory

            # Mock database engine
            mock_engine = AsyncMock()
            mock_create_engine.return_value = mock_engine

            # Mock sync engine and results
            mock_sync_engine = AsyncMock()
            mock_sync_engine_class.return_value = mock_sync_engine

            # Create mock sync result
            from mbx_db.sync import SyncResult, UpsertResult
            from datetime import datetime

            mock_table_result = UpsertResult(
                table_name="network.elements",
                records_processed=10,
                records_created=5,
                records_updated=3,
                records_failed=0,
                duration_seconds=1.5,
                errors=[],
            )

            mock_sync_result = SyncResult(started_at=datetime.now())
            mock_sync_result.add_table_result(mock_table_result)
            mock_sync_result.completed_at = datetime.now()

            mock_sync_engine.sync_all_tables.return_value = mock_sync_result

            result = self.runner.invoke(
                app, ["sync", "--config", str(config_file), "--dry-run", "--verbose"]
            )

            assert result.exit_code == 0
            assert "Starting inventory synchronization" in result.stdout
            assert "Running in dry-run mode" in result.stdout
            assert "Dry run completed successfully" in result.stdout

            # Verify sync engine was called with correct parameters
            mock_sync_engine.sync_all_tables.assert_called_once_with(
                dry_run=True, table_filter=None
            )

        finally:
            config_file.unlink()

    @patch("sqlalchemy.ext.asyncio.create_async_engine")
    @patch("mbx_inventory.cli.config.InventoryConfig.get_inventory_instance")
    @patch("mbx_inventory.cli.sync_engine.SyncEngine")
    def test_sync_command_with_table_filter_integration(
        self, mock_sync_engine_class, mock_get_inventory, mock_create_engine
    ):
        """Test sync command with table filter integration."""
        config_file = self.create_test_config_file(self.config_data)

        try:
            # Mock inventory instance
            mock_inventory = MagicMock()
            mock_get_inventory.return_value = mock_inventory

            # Mock database engine
            mock_engine = AsyncMock()
            mock_create_engine.return_value = mock_engine

            # Mock sync engine and results
            mock_sync_engine = AsyncMock()
            mock_sync_engine_class.return_value = mock_sync_engine

            # Create mock sync result
            from mbx_db.sync import SyncResult, UpsertResult
            from datetime import datetime

            mock_table_result = UpsertResult(
                table_name="network.elements",
                records_processed=10,
                records_created=5,
                records_updated=3,
                records_failed=0,
                duration_seconds=1.5,
                errors=[],
            )

            mock_sync_result = SyncResult(started_at=datetime.now())
            mock_sync_result.add_table_result(mock_table_result)
            mock_sync_result.completed_at = datetime.now()

            mock_sync_engine.sync_all_tables.return_value = mock_sync_result

            result = self.runner.invoke(
                app,
                [
                    "sync",
                    "--config",
                    str(config_file),
                    "--tables",
                    "elements,stations",
                    "--verbose",
                ],
            )

            assert result.exit_code == 0
            assert "Starting inventory synchronization" in result.stdout
            assert "Tables to sync" in result.stdout
            assert "elements" in result.stdout
            assert "stations" in result.stdout

            # Verify sync engine was called with correct table filter
            mock_sync_engine.sync_all_tables.assert_called_once_with(
                dry_run=False, table_filter=["elements", "stations"]
            )

        finally:
            config_file.unlink()

    def test_validate_command_with_missing_config_file(self):
        """Test validate command with missing config file."""
        result = self.runner.invoke(
            app, ["validate", "--config", "nonexistent_config.json"]
        )

        assert result.exit_code != 0
        # The error should be caught by typer for file existence

    def test_sync_command_with_invalid_config(self):
        """Test sync command with invalid configuration."""
        # Create invalid config (missing required fields)
        invalid_config = {
            "backend": {
                "type": "airtable"
                # Missing config section
            }
        }

        config_file = self.create_test_config_file(invalid_config)

        try:
            result = self.runner.invoke(app, ["sync", "--config", str(config_file)])

            assert result.exit_code == 1
            assert "Error loading configuration" in result.stdout

        finally:
            config_file.unlink()
