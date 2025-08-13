"""Configuration management for inventory CLI."""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from ..backends import AirtableBackend, BaserowBackend, NocoDBBackend
from ..inventory import Inventory
from .exceptions import (
    ErrorContext,
    create_config_error,
    create_backend_error,
    create_database_error,
)
from .retry import retry_database_operation


class BackendConfig(BaseModel):
    """Configuration for inventory backend."""

    type: str = Field(..., description="Backend type (airtable, baserow, nocodb)")
    config: Dict[str, Any] = Field(..., description="Backend-specific configuration")

    @field_validator("type")
    @classmethod
    def validate_backend_type(cls, v):
        """Validate backend type is supported."""
        supported_types = ["airtable", "baserow", "nocodb"]
        if v not in supported_types:
            raise ValueError(f"Backend type must be one of: {supported_types}")
        return v

    @field_validator("config")
    @classmethod
    def validate_backend_config(cls, v, info):
        """Validate backend-specific configuration."""
        backend_type = info.data.get("type")

        if backend_type == "airtable":
            required_keys = ["api_key", "base_id"]
            missing_keys = [key for key in required_keys if key not in v]
            if missing_keys:
                raise ValueError(
                    f"AirTable backend missing required config keys: {missing_keys}"
                )

        elif backend_type == "baserow":
            required_keys = ["api_key", "base_url"]
            missing_keys = [key for key in required_keys if key not in v]
            if missing_keys:
                raise ValueError(
                    f"Baserow backend missing required config keys: {missing_keys}"
                )

        elif backend_type == "nocodb":
            required_keys = ["api_key", "base_url"]
            missing_keys = [key for key in required_keys if key not in v]
            if missing_keys:
                raise ValueError(
                    f"NocoDB backend missing required config keys: {missing_keys}"
                )

        return v


class DatabaseConfig(BaseModel):
    """Configuration for PostgreSQL database connection."""

    host: str = Field(..., description="Database host")
    port: int = Field(default=5432, description="Database port", ge=1, le=65535)
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")

    @field_validator("host")
    @classmethod
    def validate_host(cls, v):
        """Validate host is not empty."""
        if not v.strip():
            raise ValueError("Database host cannot be empty")
        return v.strip()

    @field_validator("database", "username")
    @classmethod
    def validate_required_fields(cls, v):
        """Validate required fields are not empty."""
        if not v.strip():
            raise ValueError("Database name and username cannot be empty")
        return v.strip()


class SyncOptions(BaseModel):
    """Options for synchronization operations."""

    batch_size: int = Field(
        default=100, description="Batch size for processing records", ge=1, le=1000
    )
    timeout: int = Field(
        default=30, description="Timeout in seconds for operations", ge=1, le=300
    )
    retry_attempts: int = Field(
        default=3,
        description="Number of retry attempts for failed operations",
        ge=0,
        le=10,
    )


class TableConfig(BaseModel):
    """Configuration for a specific table sync."""

    backend_table_name: str = Field(..., description="Name of the table in the backend")
    field_mappings: Optional[Dict[str, str]] = Field(
        default=None, description="Mapping of schema field names to backend field names"
    )
    enabled: bool = Field(default=True, description="Whether to sync this table")


class InventoryConfig(BaseModel):
    """Main configuration for inventory CLI."""

    backend: BackendConfig = Field(..., description="Backend configuration")
    database: DatabaseConfig = Field(..., description="Database configuration")
    table_mappings: Optional[Dict[str, str]] = Field(
        default=None,
        description="Mapping of internal table names to backend table names (deprecated - use tables config)",
    )
    tables: Optional[Dict[str, TableConfig]] = Field(
        default=None,
        description="Per-table configuration with backend names and field mappings",
    )
    sync_options: SyncOptions = Field(
        default_factory=SyncOptions, description="Synchronization options"
    )

    @classmethod
    def load_from_file(cls, config_path: Path) -> "InventoryConfig":
        """Load configuration from JSON file with environment variable substitution."""
        logger = logging.getLogger("mbx_inventory")

        context = ErrorContext(
            operation="load_configuration",
            additional_data={"config_file": str(config_path)},
        )

        if not config_path.exists():
            logger.error(f"Configuration file not found: {config_path}")
            raise create_config_error(
                f"Configuration file not found: {config_path}",
                config_file=str(config_path),
                context=context,
            )

        try:
            logger.debug(f"Loading configuration from {config_path}")
            with open(config_path, "r") as f:
                config_data = json.load(f)

            logger.debug("Performing environment variable substitution")
            # Perform environment variable substitution
            config_data = cls._substitute_env_vars(config_data)

            logger.debug("Validating configuration structure")
            config = cls(**config_data)
            logger.info("Configuration loaded successfully")
            return config

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
            raise create_config_error(
                f"Invalid JSON in configuration file: {e}",
                config_file=str(config_path),
                context=context,
            )
        except ValueError as e:
            if "Environment variable" in str(e):
                # Extract missing environment variable
                missing_var = str(e).split("'")[1] if "'" in str(e) else "unknown"
                raise create_config_error(
                    f"Missing environment variable: {missing_var}",
                    config_file=str(config_path),
                    missing_fields=[missing_var],
                    context=context,
                )
            else:
                raise create_config_error(
                    f"Configuration validation error: {e}",
                    config_file=str(config_path),
                    context=context,
                )
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise create_config_error(
                f"Unexpected error loading configuration: {e}",
                config_file=str(config_path),
                context=context,
                cause=e,
            )

    @staticmethod
    def _substitute_env_vars(data: Any) -> Any:
        """Recursively substitute environment variables in configuration data."""
        if isinstance(data, dict):
            return {
                key: InventoryConfig._substitute_env_vars(value)
                for key, value in data.items()
            }
        elif isinstance(data, list):
            return [InventoryConfig._substitute_env_vars(item) for item in data]
        elif isinstance(data, str) and data.startswith("${") and data.endswith("}"):
            env_var = data[2:-1]
            env_value = os.getenv(env_var)
            if env_value is None:
                raise ValueError(f"Environment variable '{env_var}' is not set")
            return env_value
        else:
            return data

    def validate_environment_variables(self) -> bool:
        """Validate that all required environment variables are set."""
        logger = logging.getLogger("mbx_inventory")

        try:
            logger.debug("Validating environment variables")
            # Re-substitute environment variables to check if they're all available
            config_dict = self.model_dump()
            InventoryConfig._substitute_env_vars(config_dict)
            logger.debug("All environment variables are available")
            return True
        except ValueError as e:
            logger.warning(f"Environment variable validation failed: {e}")
            return False

    def validate_connectivity(self) -> bool:
        """Validate that backend and database connections can be established."""
        try:
            # Test backend connectivity
            backend_instance = self.get_backend_instance()
            if not backend_instance.validate():
                return False

            # Test database connectivity (placeholder - will be implemented with actual DB connection)
            # For now, just validate that all required database fields are present
            if not all(
                [
                    self.database.host,
                    self.database.database,
                    self.database.username,
                    self.database.password,
                ]
            ):
                return False

            return True
        except Exception:
            return False

    @retry_database_operation("validate_database_connectivity")
    async def validate_database_connectivity(self) -> bool:
        """Validate database connection asynchronously with retry logic."""
        logger = logging.getLogger("mbx_inventory")

        context = ErrorContext(
            operation="validate_database_connectivity",
            additional_data={
                "host": self.database.host,
                "port": self.database.port,
                "database": self.database.database,
            },
        )

        try:
            logger.debug(
                f"Testing database connection to {self.database.host}:{self.database.port}"
            )

            # Import here to avoid circular imports
            from mbx_db import make_connection_string

            connection_string = make_connection_string(
                username=self.database.username,
                password=self.database.password,
                host=self.database.host,
                database=self.database.database,
                port=self.database.port,
            )

            engine = create_async_engine(connection_string)

            async with engine.connect() as conn:
                # Simple query to test connectivity
                await conn.execute(text("SELECT 1"))

            await engine.dispose()
            logger.debug("Database connection successful")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise create_database_error(
                f"Database connection failed: {e}",
                operation="connectivity_test",
                context=context,
                cause=e,
            )

    def get_backend_instance(self):
        """Get backend instance based on configuration."""
        logger = logging.getLogger("mbx_inventory")

        backend_type = self.backend.type
        config = self.backend.config

        context = ErrorContext(
            operation="create_backend_instance",
            additional_data={"backend_type": backend_type},
        )

        logger.debug(f"Creating {backend_type} backend instance")

        try:
            if backend_type == "airtable":
                return AirtableBackend(
                    api_key=config["api_key"], base_id=config["base_id"]
                )
            elif backend_type == "baserow":
                return BaserowBackend(
                    api_key=config["api_key"], base_url=config["base_url"]
                )
            elif backend_type == "nocodb":
                return NocoDBBackend(
                    api_key=config["api_key"], base_url=config["base_url"]
                )
            else:
                logger.error(f"Unsupported backend type: {backend_type}")
                raise create_backend_error(
                    f"Unsupported backend type: {backend_type}",
                    backend_type=backend_type,
                    context=context,
                )
        except KeyError as e:
            missing_key = str(e).strip("'\"")
            raise create_config_error(
                f"Missing required configuration key for {backend_type}: {missing_key}",
                missing_fields=[missing_key],
                context=context,
            )
        except Exception as e:
            raise create_backend_error(
                f"Failed to create {backend_type} backend: {e}",
                backend_type=backend_type,
                context=context,
                cause=e,
            )

    def get_inventory_instance(self) -> Inventory:
        """Get configured Inventory instance."""
        logger = logging.getLogger("mbx_inventory")

        logger.debug("Creating inventory instance")
        backend = self.get_backend_instance()

        # Use new tables config if available, otherwise fall back to table_mappings
        table_mappings = None
        table_configs = None

        if self.tables:
            # Extract table mappings and configs from new format
            table_mappings = {}
            table_configs = {}
            for schema_name, config in self.tables.items():
                if config.enabled:
                    table_mappings[schema_name] = config.backend_table_name
                    table_configs[schema_name] = config
        elif self.table_mappings:
            # Use legacy table_mappings
            table_mappings = self.table_mappings

        inventory = Inventory(
            backend=backend,
            table_mappings=table_mappings,
            table_configs=table_configs,
            backend_type=self.backend.type,
        )

        logger.debug("Inventory instance created successfully")
        return inventory

    def validate_table_mappings(self) -> bool:
        """Validate that table mappings reference existing tables in the backend."""
        if not self.table_mappings:
            return True

        try:
            backend = self.get_backend_instance()
            # This would require backend-specific table listing functionality
            # For now, just validate that mappings are not empty
            return all(mapping.strip() for mapping in self.table_mappings.values())
        except Exception:
            return False
