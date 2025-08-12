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


class ConfigurationError(Exception):
    """Base exception for configuration-related errors."""

    pass


class BackendConnectionError(ConfigurationError):
    """Exception raised when backend connection fails."""

    pass


class DatabaseConnectionError(ConfigurationError):
    """Exception raised when database connection fails."""

    pass


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


class InventoryConfig(BaseModel):
    """Main configuration for inventory CLI."""

    backend: BackendConfig = Field(..., description="Backend configuration")
    database: DatabaseConfig = Field(..., description="Database configuration")
    table_mappings: Optional[Dict[str, str]] = Field(
        default=None,
        description="Mapping of internal table names to backend table names",
    )
    sync_options: SyncOptions = Field(
        default_factory=SyncOptions, description="Synchronization options"
    )

    @classmethod
    def load_from_file(cls, config_path: Path) -> "InventoryConfig":
        """Load configuration from JSON file with environment variable substitution."""
        logger = logging.getLogger("mbx_inventory")

        if not config_path.exists():
            logger.error(f"Configuration file not found: {config_path}")
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

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
            raise ValueError(f"Invalid JSON in configuration file: {e}")
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise ValueError(f"Error loading configuration: {e}")

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

    async def validate_database_connectivity(self) -> bool:
        """Validate database connection asynchronously."""
        logger = logging.getLogger("mbx_inventory")

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
            return False

    def get_backend_instance(self):
        """Get backend instance based on configuration."""
        logger = logging.getLogger("mbx_inventory")

        backend_type = self.backend.type
        config = self.backend.config

        logger.debug(f"Creating {backend_type} backend instance")

        if backend_type == "airtable":
            return AirtableBackend(api_key=config["api_key"], base_id=config["base_id"])
        elif backend_type == "baserow":
            return BaserowBackend(
                api_key=config["api_key"], base_url=config["base_url"]
            )
        elif backend_type == "nocodb":
            return NocoDBBackend(api_key=config["api_key"], base_url=config["base_url"])
        else:
            logger.error(f"Unsupported backend type: {backend_type}")
            raise ValueError(f"Unsupported backend type: {backend_type}")

    def get_inventory_instance(self) -> Inventory:
        """Get configured Inventory instance."""
        logger = logging.getLogger("mbx_inventory")

        logger.debug("Creating inventory instance")
        backend = self.get_backend_instance()

        inventory = Inventory(
            backend=backend,
            table_mappings=self.table_mappings,
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
