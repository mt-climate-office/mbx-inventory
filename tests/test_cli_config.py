"""Tests for CLI configuration management."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from mbx_inventory.cli.config import (
    InventoryConfig,
    BackendConfig,
    DatabaseConfig,
    SyncOptions,
)
from mbx_inventory.cli.main import app


class TestBackendConfig:
    """Test BackendConfig validation."""

    def test_valid_airtable_config(self):
        """Test valid AirTable backend configuration."""
        config = BackendConfig(
            type="airtable", config={"api_key": "test_key", "base_id": "test_base"}
        )
        assert config.type == "airtable"
        assert config.config["api_key"] == "test_key"

    def test_valid_baserow_config(self):
        """Test valid Baserow backend configuration."""
        config = BackendConfig(
            type="baserow",
            config={"api_key": "test_key", "base_url": "https://baserow.example.com"},
        )
        assert config.type == "baserow"
        assert config.config["base_url"] == "https://baserow.example.com"

    def test_valid_nocodb_config(self):
        """Test valid NocoDB backend configuration."""
        config = BackendConfig(
            type="nocodb",
            config={"api_key": "test_key", "base_url": "https://nocodb.example.com"},
        )
        assert config.type == "nocodb"

    def test_invalid_backend_type(self):
        """Test invalid backend type raises validation error."""
        with pytest.raises(ValidationError) as exc_info:
            BackendConfig(type="invalid_backend", config={"api_key": "test_key"})
        assert "Backend type must be one of" in str(exc_info.value)

    def test_missing_airtable_config_keys(self):
        """Test missing required AirTable config keys."""
        with pytest.raises(ValidationError) as exc_info:
            BackendConfig(
                type="airtable",
                config={"api_key": "test_key"},  # missing base_id
            )
        assert "missing required config keys" in str(exc_info.value)

    def test_missing_baserow_config_keys(self):
        """Test missing required Baserow config keys."""
        with pytest.raises(ValidationError) as exc_info:
            BackendConfig(
                type="baserow",
                config={"api_key": "test_key"},  # missing base_url
            )
        assert "missing required config keys" in str(exc_info.value)


class TestDatabaseConfig:
    """Test DatabaseConfig validation."""

    def test_valid_database_config(self):
        """Test valid database configuration."""
        config = DatabaseConfig(
            host="localhost",
            port=5432,
            database="test_db",
            username="test_user",
            password="test_pass",
        )
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "test_db"

    def test_default_port(self):
        """Test default port is set correctly."""
        config = DatabaseConfig(
            host="localhost",
            database="test_db",
            username="test_user",
            password="test_pass",
        )
        assert config.port == 5432

    def test_invalid_port_range(self):
        """Test invalid port range raises validation error."""
        with pytest.raises(ValidationError):
            DatabaseConfig(
                host="localhost",
                port=70000,  # Invalid port
                database="test_db",
                username="test_user",
                password="test_pass",
            )

    def test_empty_host(self):
        """Test empty host raises validation error."""
        with pytest.raises(ValidationError):
            DatabaseConfig(
                host="", database="test_db", username="test_user", password="test_pass"
            )

    def test_empty_database_name(self):
        """Test empty database name raises validation error."""
        with pytest.raises(ValidationError):
            DatabaseConfig(
                host="localhost",
                database="",
                username="test_user",
                password="test_pass",
            )


class TestSyncOptions:
    """Test SyncOptions validation."""

    def test_default_values(self):
        """Test default sync options values."""
        options = SyncOptions()
        assert options.batch_size == 100
        assert options.timeout == 30
        assert options.retry_attempts == 3

    def test_custom_values(self):
        """Test custom sync options values."""
        options = SyncOptions(batch_size=50, timeout=60, retry_attempts=5)
        assert options.batch_size == 50
        assert options.timeout == 60
        assert options.retry_attempts == 5

    def test_invalid_batch_size(self):
        """Test invalid batch size raises validation error."""
        with pytest.raises(ValidationError):
            SyncOptions(batch_size=0)  # Too small

        with pytest.raises(ValidationError):
            SyncOptions(batch_size=2000)  # Too large

    def test_invalid_timeout(self):
        """Test invalid timeout raises validation error."""
        with pytest.raises(ValidationError):
            SyncOptions(timeout=0)  # Too small

        with pytest.raises(ValidationError):
            SyncOptions(timeout=500)  # Too large


class TestInventoryConfig:
    """Test InventoryConfig functionality."""

    def create_test_config_file(self, config_data: dict) -> Path:
        """Create a temporary config file for testing."""
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump(config_data, temp_file)
        temp_file.close()
        return Path(temp_file.name)

    def test_load_valid_config(self):
        """Test loading valid configuration from file."""
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

        config_file = self.create_test_config_file(config_data)
        try:
            config = InventoryConfig.load_from_file(config_file)
            assert config.backend.type == "airtable"
            assert config.database.host == "localhost"
        finally:
            config_file.unlink()

    def test_load_config_with_env_vars(self):
        """Test loading configuration with environment variable substitution."""
        config_data = {
            "backend": {
                "type": "airtable",
                "config": {"api_key": "${TEST_API_KEY}", "base_id": "${TEST_BASE_ID}"},
            },
            "database": {
                "host": "${TEST_DB_HOST}",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass",
            },
        }

        config_file = self.create_test_config_file(config_data)
        try:
            with patch.dict(
                os.environ,
                {
                    "TEST_API_KEY": "actual_key",
                    "TEST_BASE_ID": "actual_base",
                    "TEST_DB_HOST": "actual_host",
                },
            ):
                config = InventoryConfig.load_from_file(config_file)
                assert config.backend.config["api_key"] == "actual_key"
                assert config.backend.config["base_id"] == "actual_base"
                assert config.database.host == "actual_host"
        finally:
            config_file.unlink()

    def test_missing_env_var_raises_error(self):
        """Test that missing environment variables raise an error."""
        config_data = {
            "backend": {
                "type": "airtable",
                "config": {"api_key": "${MISSING_API_KEY}", "base_id": "test_base"},
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass",
            },
        }

        config_file = self.create_test_config_file(config_data)
        try:
            with pytest.raises(ValueError) as exc_info:
                InventoryConfig.load_from_file(config_file)
            assert "Environment variable 'MISSING_API_KEY' is not set" in str(
                exc_info.value
            )
        finally:
            config_file.unlink()

    def test_file_not_found(self):
        """Test loading non-existent config file raises error."""
        with pytest.raises(FileNotFoundError):
            InventoryConfig.load_from_file(Path("non_existent_config.json"))

    def test_invalid_json(self):
        """Test loading invalid JSON raises error."""
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        temp_file.write("invalid json content")
        temp_file.close()

        try:
            with pytest.raises(ValueError) as exc_info:
                InventoryConfig.load_from_file(Path(temp_file.name))
            assert "Invalid JSON" in str(exc_info.value)
        finally:
            Path(temp_file.name).unlink()

    @patch("mbx_inventory.cli.config.AirtableBackend")
    def test_get_backend_instance_airtable(self, mock_airtable):
        """Test getting AirTable backend instance."""
        config = InventoryConfig(
            backend=BackendConfig(
                type="airtable", config={"api_key": "test_key", "base_id": "test_base"}
            ),
            database=DatabaseConfig(
                host="localhost",
                database="test_db",
                username="test_user",
                password="test_pass",
            ),
        )

        backend = config.get_backend_instance()
        mock_airtable.assert_called_once_with(api_key="test_key", base_id="test_base")

    @patch("mbx_inventory.cli.config.BaserowBackend")
    def test_get_backend_instance_baserow(self, mock_baserow):
        """Test getting Baserow backend instance."""
        config = InventoryConfig(
            backend=BackendConfig(
                type="baserow",
                config={
                    "api_key": "test_key",
                    "base_url": "https://baserow.example.com",
                },
            ),
            database=DatabaseConfig(
                host="localhost",
                database="test_db",
                username="test_user",
                password="test_pass",
            ),
        )

        backend = config.get_backend_instance()
        mock_baserow.assert_called_once_with(
            api_key="test_key", base_url="https://baserow.example.com"
        )

    @patch("mbx_inventory.cli.config.NocoDBBackend")
    def test_get_backend_instance_nocodb(self, mock_nocodb):
        """Test getting NocoDB backend instance."""
        config = InventoryConfig(
            backend=BackendConfig(
                type="nocodb",
                config={
                    "api_key": "test_key",
                    "base_url": "https://nocodb.example.com",
                },
            ),
            database=DatabaseConfig(
                host="localhost",
                database="test_db",
                username="test_user",
                password="test_pass",
            ),
        )

        backend = config.get_backend_instance()
        mock_nocodb.assert_called_once_with(
            api_key="test_key", base_url="https://nocodb.example.com"
        )

    def test_validate_environment_variables(self):
        """Test environment variable validation."""
        config_data = {
            "backend": {
                "type": "airtable",
                "config": {"api_key": "${TEST_API_KEY}", "base_id": "test_base"},
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass",
            },
        }

        config_file = self.create_test_config_file(config_data)
        try:
            with patch.dict(os.environ, {"TEST_API_KEY": "actual_key"}):
                config = InventoryConfig.load_from_file(config_file)
                assert config.validate_environment_variables() is True
        finally:
            config_file.unlink()

    @patch("mbx_inventory.cli.config.AirtableBackend")
    def test_validate_connectivity_success(self, mock_airtable):
        """Test successful connectivity validation."""
        mock_backend = MagicMock()
        mock_backend.validate.return_value = True
        mock_airtable.return_value = mock_backend

        config = InventoryConfig(
            backend=BackendConfig(
                type="airtable", config={"api_key": "test_key", "base_id": "test_base"}
            ),
            database=DatabaseConfig(
                host="localhost",
                database="test_db",
                username="test_user",
                password="test_pass",
            ),
        )

        assert config.validate_connectivity() is True

    @patch("mbx_inventory.cli.config.AirtableBackend")
    def test_validate_connectivity_failure(self, mock_airtable):
        """Test failed connectivity validation."""
        mock_backend = MagicMock()
        mock_backend.validate.return_value = False
        mock_airtable.return_value = mock_backend

        config = InventoryConfig(
            backend=BackendConfig(
                type="airtable", config={"api_key": "test_key", "base_id": "test_base"}
            ),
            database=DatabaseConfig(
                host="localhost",
                database="test_db",
                username="test_user",
                password="test_pass",
            ),
        )

        assert config.validate_connectivity() is False

    def test_validate_database_connectivity_method_exists(self):
        """Test that validate_database_connectivity method exists and is callable."""
        config = InventoryConfig(
            backend=BackendConfig(
                type="airtable", config={"api_key": "test_key", "base_id": "test_base"}
            ),
            database=DatabaseConfig(
                host="localhost",
                database="test_db",
                username="test_user",
                password="test_pass",
            ),
        )

        # Just verify the method exists and is callable
        assert hasattr(config, "validate_database_connectivity")
        assert callable(getattr(config, "validate_database_connectivity"))


class TestCLICommands:
    """Test CLI command functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()
        self.config_data = {
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

    def create_test_config_file(self, config_data: dict) -> Path:
        """Create a temporary config file for testing."""
        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump(config_data, temp_file)
        temp_file.close()
        return Path(temp_file.name)

    @patch("mbx_inventory.cli.config.AirtableBackend")
    @patch("asyncio.run")
    def test_validate_command_success(self, mock_asyncio_run, mock_airtable):
        """Test successful validate command."""
        mock_backend = MagicMock()
        mock_backend.validate.return_value = True
        mock_airtable.return_value = mock_backend
        mock_asyncio_run.return_value = True  # Database connectivity success

        config_file = self.create_test_config_file(self.config_data)
        try:
            result = self.runner.invoke(app, ["validate", "--config", str(config_file)])
            assert result.exit_code == 0
            # Updated to match new logging format
            assert "All validation checks passed" in result.stdout
            assert "Configuration loaded successfully" in result.stdout
            assert "Backend connection successful" in result.stdout
            assert "Database connection successful" in result.stdout
        finally:
            config_file.unlink()

    @patch("mbx_inventory.cli.config.AirtableBackend")
    def test_validate_command_backend_failure(self, mock_airtable):
        """Test validate command with backend connection failure."""
        mock_backend = MagicMock()
        mock_backend.validate.return_value = False
        mock_airtable.return_value = mock_backend

        config_file = self.create_test_config_file(self.config_data)
        try:
            result = self.runner.invoke(app, ["validate", "--config", str(config_file)])
            assert result.exit_code == 1
            assert "Backend connection failed" in result.stdout
        finally:
            config_file.unlink()

    @patch("mbx_inventory.cli.config.AirtableBackend")
    @patch("asyncio.run")
    def test_validate_command_database_failure(self, mock_asyncio_run, mock_airtable):
        """Test validate command with database connection failure."""
        mock_backend = MagicMock()
        mock_backend.validate.return_value = True
        mock_airtable.return_value = mock_backend
        mock_asyncio_run.return_value = False  # Database connectivity failure

        config_file = self.create_test_config_file(self.config_data)
        try:
            result = self.runner.invoke(app, ["validate", "--config", str(config_file)])
            assert result.exit_code == 1
            assert "Database connection failed" in result.stdout
        finally:
            config_file.unlink()

    def test_config_show_command(self):
        """Test config show command."""
        config_file = self.create_test_config_file(self.config_data)
        try:
            result = self.runner.invoke(
                app, ["config", "show", "--config", str(config_file)]
            )
            assert result.exit_code == 0
            assert "Backend Configuration" in result.stdout
            assert "Database Configuration" in result.stdout
            assert "airtable" in result.stdout
            assert "localhost" in result.stdout
        finally:
            config_file.unlink()

    def test_config_validate_command_basic(self):
        """Test basic config validate command."""
        config_file = self.create_test_config_file(self.config_data)
        try:
            result = self.runner.invoke(
                app, ["config", "validate", "--config", str(config_file)]
            )
            assert result.exit_code == 0
            assert "Configuration file is valid" in result.stdout
            assert "airtable" in result.stdout
        finally:
            config_file.unlink()

    @patch("mbx_inventory.cli.config.AirtableBackend")
    @patch("asyncio.run")
    def test_config_validate_command_with_connectivity(
        self, mock_asyncio_run, mock_airtable
    ):
        """Test config validate command with connectivity testing."""
        mock_backend = MagicMock()
        mock_backend.validate.return_value = True
        mock_airtable.return_value = mock_backend
        mock_asyncio_run.return_value = True  # Database connectivity success

        config_file = self.create_test_config_file(self.config_data)
        try:
            result = self.runner.invoke(
                app,
                [
                    "config",
                    "validate",
                    "--config",
                    str(config_file),
                    "--test-connectivity",
                ],
            )
            assert result.exit_code == 0
            assert "Backend connection successful" in result.stdout
            assert "Database connection successful" in result.stdout
        finally:
            config_file.unlink()

    def test_config_validate_invalid_file(self):
        """Test config validate with invalid configuration file."""
        invalid_config = {"invalid": "config"}
        config_file = self.create_test_config_file(invalid_config)
        try:
            result = self.runner.invoke(
                app, ["config", "validate", "--config", str(config_file)]
            )
            assert result.exit_code == 1
            assert "Error" in result.stdout
        finally:
            config_file.unlink()

    def test_config_validate_missing_file(self):
        """Test config validate with missing configuration file."""
        result = self.runner.invoke(
            app, ["config", "validate", "--config", "nonexistent.json"]
        )
        assert result.exit_code == 2  # Typer file not found error

    @patch.dict(os.environ, {}, clear=True)
    def test_config_with_missing_env_vars(self):
        """Test configuration with missing environment variables."""
        config_data_with_env = {
            "backend": {
                "type": "airtable",
                "config": {"api_key": "${MISSING_API_KEY}", "base_id": "test_base"},
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass",
            },
        }

        config_file = self.create_test_config_file(config_data_with_env)
        try:
            result = self.runner.invoke(
                app, ["config", "validate", "--config", str(config_file)]
            )
            assert result.exit_code == 1
            assert "MISSING_API_KEY" in result.stdout and "not set" in result.stdout
        finally:
            config_file.unlink()
