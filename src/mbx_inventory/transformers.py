"""
Data transformers for converting inventory backend data to network schema format.

This module provides base classes and specific transformers for each network schema table,
handling data validation, type conversion, and field mapping from inventory backends
(primarily AirTable) to the database schema format.
"""

from typing import Dict, List, Any, Optional, Type
from datetime import date, datetime
import logging

logger = logging.getLogger(__name__)


# Exception Hierarchy
class NetworkSchemaError(Exception):
    """Base exception for network schema operations."""

    pass


class ValidationError(NetworkSchemaError):
    """Raised when data validation fails."""

    pass


class TransformationError(NetworkSchemaError):
    """Raised when data transformation fails."""

    pass


class BackendError(NetworkSchemaError):
    """Raised when backend operations fail."""

    pass


class BaseTransformer:
    """Base class for data transformers.

    Provides common functionality for transforming raw backend data
    to network schema format, including validation and field mapping.
    """

    # Subclasses should define these
    REQUIRED_FIELDS: List[str] = []
    OPTIONAL_FIELDS: List[str] = []
    FIELD_MAPPINGS: Dict[str, str] = {}  # schema_field -> backend_field
    FIELD_TYPES: Dict[str, Type] = {}  # schema_field -> target_type

    @classmethod
    def transform(cls, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform raw backend data to network schema format.

        Args:
            raw_data: List of dictionaries from inventory backend

        Returns:
            List of dictionaries formatted for network schema

        Raises:
            ValidationError: If required fields are missing
            TransformationError: If data transformation fails
        """
        if not raw_data:
            return []

        transformed_data = []

        for record in raw_data:
            try:
                transformed_record = cls._transform_record(record)
                transformed_data.append(transformed_record)
            except Exception as e:
                logger.error(
                    f"Failed to transform record {record.get('id', 'unknown')}: {e}"
                )
                raise TransformationError(f"Failed to transform record: {e}") from e

        return transformed_data

    @classmethod
    def _transform_record(cls, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single record.

        Args:
            record: Single record dictionary from backend

        Returns:
            Transformed record dictionary
        """
        # Start with empty transformed record
        transformed = {}

        # Map required fields
        missing_fields = []
        for field in cls.REQUIRED_FIELDS:
            backend_field = cls.FIELD_MAPPINGS.get(field, field)
            if backend_field in record and record[backend_field] is not None:
                transformed[field] = cls._convert_field_value(
                    field, record[backend_field]
                )
            else:
                missing_fields.append(f"'{field}' (backend: '{backend_field}')")

        if missing_fields:
            raise ValidationError(
                f"Required fields not found in record: {', '.join(missing_fields)}"
            )

        # Map optional fields
        for field in cls.OPTIONAL_FIELDS:
            backend_field = cls.FIELD_MAPPINGS.get(field, field)
            if backend_field in record and record[backend_field] is not None:
                transformed[field] = cls._convert_field_value(
                    field, record[backend_field]
                )

        # Handle extra fields if extra_data is supported
        if "extra_data" in cls.OPTIONAL_FIELDS:
            extra_data = cls._extract_extra_fields(record)
            if extra_data:
                transformed["extra_data"] = extra_data

        return transformed

    @classmethod
    def _convert_field_value(cls, field_name: str, value: Any) -> Any:
        """Convert field value to appropriate type.

        Args:
            field_name: Name of the field being converted
            value: Raw value from backend

        Returns:
            Converted value
        """
        # Check if we have a specific type defined for this field
        if field_name in cls.FIELD_TYPES:
            target_type = cls.FIELD_TYPES[field_name]
            return cls.convert_value(value, target_type)

        # Default implementation - return as-is
        return value

    @classmethod
    def _extract_extra_fields(cls, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract fields not in the schema to include in extra_data.

        Args:
            record: Raw record from backend

        Returns:
            Dictionary of extra fields or None if no extra fields
        """
        all_schema_fields = set(cls.REQUIRED_FIELDS + cls.OPTIONAL_FIELDS)
        all_backend_fields = (
            set(cls.FIELD_MAPPINGS.values())
            if cls.FIELD_MAPPINGS
            else all_schema_fields
        )

        extra_fields = {}
        for key, value in record.items():
            # Skip AirTable ID field and fields already mapped to schema
            if key == "id" or key in all_backend_fields:
                continue
            # Skip fields that map to schema fields
            if key in cls.FIELD_MAPPINGS:
                continue
            extra_fields[key] = value

        return extra_fields if extra_fields else None

    @staticmethod
    def validate_required_fields(
        data: Dict[str, Any], required_fields: List[str]
    ) -> None:
        """Validate that required fields are present in the data.

        Args:
            data: Data dictionary to validate
            required_fields: List of required field names

        Raises:
            ValidationError: If any required fields are missing
        """
        missing_fields = []
        for field in required_fields:
            if field not in data or data[field] is None:
                missing_fields.append(field)

        if missing_fields:
            raise ValidationError(
                f"Missing required fields: {', '.join(missing_fields)}"
            )

    @staticmethod
    def convert_value(value: Any, target_type: Type) -> Any:
        """Convert value to the specified type with error handling.

        Args:
            value: Value to convert
            target_type: Target type to convert to (int, float, date, str, etc.)

        Returns:
            Converted value or None if conversion fails

        Examples:
            convert_value("42", int) -> 42
            convert_value("2023-12-25", date) -> date(2023, 12, 25)
            convert_value(42.5, float) -> 42.5
        """
        if value is None:
            return None

        # If already the correct type, return as-is
        if isinstance(value, target_type):
            # Special case for date vs datetime
            if target_type == date and isinstance(value, datetime):
                return value.date()
            return value

        try:
            # Handle date conversion specially
            if target_type == date:
                return BaseTransformer._convert_to_date(value)

            # Handle string conversion
            if target_type == str:
                return str(value)

            # Handle numeric conversions
            if target_type in (int, float):
                return target_type(value)

            # For other types, try direct conversion
            return target_type(value)

        except (ValueError, TypeError) as e:
            logger.warning(
                f"Could not convert value '{value}' to {target_type.__name__}: {e}"
            )
            return None

    @staticmethod
    def _convert_to_date(value: Any) -> Optional[date]:
        """Internal helper for date conversion.

        Args:
            value: Date value in various formats

        Returns:
            date object or None if conversion fails
        """
        if isinstance(value, date) and not isinstance(value, datetime):
            return value

        if isinstance(value, datetime):
            return value.date()

        if isinstance(value, str):
            # Try common date formats
            for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"]:
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue

        return None


class ElementsTransformer(BaseTransformer):
    """Transformer for elements data from inventory backends to network schema format.

    Transforms inventory backend data to match the network.elements table schema.
    Required fields: element, description, description_short
    Optional fields: si_units, us_units, extra_data
    """

    REQUIRED_FIELDS = ["element", "description", "description_short"]
    OPTIONAL_FIELDS = ["si_units", "us_units", "extra_data"]

    # Default field mappings - can be overridden for specific backends
    FIELD_MAPPINGS = {
        "element": "Element",
        "description": "Description",
        "description_short": "Description Short",
        "si_units": "SI Units",
        "us_units": "US Units",
    }

    # Field types for automatic conversion
    FIELD_TYPES = {
        "element": str,
        "description": str,
        "description_short": str,
        "si_units": str,
        "us_units": str,
    }


class StationsTransformer(BaseTransformer):
    """Transformer for stations data from inventory backends to network schema format.

    Transforms inventory backend data to match the network.stations table schema.
    Required fields: station, name, status, latitude, longitude, elevation
    Optional fields: date_installed, extra_data
    """

    REQUIRED_FIELDS = [
        "station",
        "name",
        "status",
        "latitude",
        "longitude",
        "elevation",
    ]
    OPTIONAL_FIELDS = ["date_installed", "extra_data"]

    # Valid status values as defined in the database constraint
    VALID_STATUSES = ["pending", "active", "decommissioned", "inactive"]

    # Default field mappings - can be overridden for specific backends
    FIELD_MAPPINGS = {
        "station": "Station",
        "name": "Name",
        "status": "Status",
        "latitude": "Latitude",
        "longitude": "Longitude",
        "elevation": "Elevation",
        "date_installed": "Date Installed",
    }

    # Field types for automatic conversion
    FIELD_TYPES = {
        "station": str,
        "name": str,
        "status": str,
        "latitude": float,
        "longitude": float,
        "elevation": float,
        "date_installed": date,
    }

    @classmethod
    def _convert_field_value(cls, field_name: str, value: Any) -> Any:
        """Convert field value with additional validation for stations.

        Args:
            field_name: Name of the field being converted
            value: Raw value from backend

        Returns:
            Converted value

        Raises:
            ValidationError: If status value is invalid or required numeric conversion fails
        """
        # Use parent conversion first
        converted_value = super()._convert_field_value(field_name, value)

        # Additional validation for status field
        if field_name == "status" and converted_value is not None:
            if converted_value not in cls.VALID_STATUSES:
                raise ValidationError(
                    f"Invalid status value '{converted_value}'. "
                    f"Must be one of: {', '.join(cls.VALID_STATUSES)}"
                )

        # Validate that required numeric fields were successfully converted
        if (
            field_name in ["latitude", "longitude", "elevation"]
            and field_name in cls.REQUIRED_FIELDS
        ):
            if converted_value is None and value is not None:
                raise ValidationError(
                    f"Could not convert required field '{field_name}' value '{value}' to float"
                )

        return converted_value


class ComponentModelsTransformer(BaseTransformer):
    """Transformer for component models data from inventory backends to network schema format.

    Transforms inventory backend data to match the network.component_models table schema.
    Required fields: model, manufacturer, type
    Optional fields: extra_data
    """

    REQUIRED_FIELDS = ["model", "manufacturer", "type"]
    OPTIONAL_FIELDS = ["extra_data"]

    # Default field mappings - can be overridden for specific backends
    FIELD_MAPPINGS = {
        "model": "Model",
        "manufacturer": "Manufacturer",
        "type": "Type",
    }

    # Field types for automatic conversion
    FIELD_TYPES = {
        "model": str,
        "manufacturer": str,
        "type": str,
    }


class InventoryTransformer(BaseTransformer):
    """Transformer for inventory data from inventory backends to network schema format.

    Transforms inventory backend data to match the network.inventory table schema.
    Required fields: model, serial_number
    Optional fields: extra_data
    """

    REQUIRED_FIELDS = ["model", "serial_number"]
    OPTIONAL_FIELDS = ["extra_data"]

    # Default field mappings - can be overridden for specific backends
    FIELD_MAPPINGS = {
        "model": "Model",
        "serial_number": "Serial Number",
    }

    # Field types for automatic conversion
    FIELD_TYPES = {
        "model": str,
        "serial_number": str,
    }


class DeploymentsTransformer(BaseTransformer):
    """Transformer for deployments data from inventory backends to network schema format.

    Transforms inventory backend data to match the network.deployments table schema.
    Required fields: station, model, serial_number, date_assigned
    Optional fields: date_start, date_end, extra_data, elevation_cm
    """

    REQUIRED_FIELDS = ["station", "model", "serial_number", "date_assigned"]
    OPTIONAL_FIELDS = ["date_start", "date_end", "extra_data", "elevation_cm"]

    # Default field mappings - can be overridden for specific backends
    FIELD_MAPPINGS = {
        "station": "Station",
        "model": "Model",
        "serial_number": "Serial Number",
        "date_assigned": "Date Assigned",
        "date_start": "Date Start",
        "date_end": "Date End",
        "elevation_cm": "Elevation (cm)",
    }

    # Field types for automatic conversion
    FIELD_TYPES = {
        "station": str,
        "model": str,
        "serial_number": str,
        "date_assigned": date,
        "date_start": date,
        "date_end": date,
        "elevation_cm": int,
    }


class ComponentElementsTransformer(BaseTransformer):
    """Transformer for component-element relationships from inventory backends to network schema format.

    Transforms inventory backend data to match the network.component_elements table schema.
    Required fields: model, element
    Optional fields: qc_values, extra_data
    """

    REQUIRED_FIELDS = ["model", "element"]
    OPTIONAL_FIELDS = ["qc_values", "extra_data"]

    # Default field mappings - can be overridden for specific backends
    FIELD_MAPPINGS = {
        "model": "Model",
        "element": "Element",
        "qc_values": "QC Values",
    }

    # Field types for automatic conversion
    FIELD_TYPES = {
        "model": str,
        "element": str,
        # qc_values is JSONB, so we'll leave it as-is (dict/list)
    }


class RequestSchemasTransformer(BaseTransformer):
    """Transformer for request schemas data from inventory backends to network schema format.

    Transforms inventory backend data to match the network.request_schemas table schema.
    Required fields: network, request_model
    Optional fields: extra_data
    """

    REQUIRED_FIELDS = ["network", "request_model"]
    OPTIONAL_FIELDS = ["extra_data"]

    # Default field mappings - can be overridden for specific backends
    FIELD_MAPPINGS = {
        "network": "Network",
        "request_model": "Request Model",
    }

    # Field types for automatic conversion
    FIELD_TYPES = {
        "network": str,
        # request_model is JSONB, so we'll leave it as-is (dict/list)
    }


class ResponseSchemasTransformer(BaseTransformer):
    """Transformer for response schemas data from inventory backends to network schema format.

    Transforms inventory backend data to match the network.response_schemas table schema.
    Required fields: response_name, response_model
    Optional fields: extra_data
    """

    REQUIRED_FIELDS = ["response_name", "response_model"]
    OPTIONAL_FIELDS = ["extra_data"]

    # Default field mappings - can be overridden for specific backends
    FIELD_MAPPINGS = {
        "response_name": "Response Name",
        "response_model": "Response Model",
    }

    # Field types for automatic conversion
    FIELD_TYPES = {
        "response_name": str,
        # response_model is JSONB, so we'll leave it as-is (dict/list)
    }


class TableNameMapper:
    """Maps network schema table names to inventory backend table names.

    This class handles the mapping between standardized network schema table names
    and the actual table names used in inventory backends like AirTable, which may
    use different naming conventions (spaces, capitalization, etc.).
    """

    # Default mappings for network schema tables
    DEFAULT_MAPPINGS = {
        "elements": "Elements",
        "component_models": "Component Models",
        "stations": "Stations",
        "inventory": "Inventory",
        "deployments": "Deployments",
        "component_elements": "Component Elements",
        "request_schemas": "Request Schemas",
        "response_schemas": "Response Schemas",
    }

    def __init__(self, custom_mappings: dict = None, backend_type: str = "airtable"):
        """Initialize the table name mapper.

        Args:
            custom_mappings: Optional dictionary of custom table name mappings
                           that override the defaults. Format: {schema_name: backend_name}
            backend_type: Type of backend ("airtable", "baserow", "nocodb")
        """
        # Start with default mappings
        self.mappings = self.DEFAULT_MAPPINGS.copy()

        # Apply any custom mappings (these override defaults and backend-specific)
        if custom_mappings:
            self.mappings.update(custom_mappings)

    def get_backend_table_name(self, schema_table_name: str) -> str:
        """Get the backend table name for a given schema table name.

        Args:
            schema_table_name: The standardized schema table name (e.g., "elements")

        Returns:
            The corresponding backend table name (e.g., "Elements" for AirTable)

        Raises:
            KeyError: If the schema table name is not found in mappings
        """
        if schema_table_name not in self.mappings:
            raise KeyError(
                f"No mapping found for schema table '{schema_table_name}'. "
                f"Available mappings: {list(self.mappings.keys())}"
            )

        return self.mappings[schema_table_name]

    def get_schema_table_name(self, backend_table_name: str) -> str:
        """Get the schema table name for a given backend table name.

        Args:
            backend_table_name: The backend table name (e.g., "Elements")

        Returns:
            The corresponding schema table name (e.g., "elements")

        Raises:
            KeyError: If the backend table name is not found in mappings
        """
        # Create reverse mapping
        reverse_mappings = {v: k for k, v in self.mappings.items()}

        if backend_table_name not in reverse_mappings:
            raise KeyError(
                f"No mapping found for backend table '{backend_table_name}'. "
                f"Available backend tables: {list(reverse_mappings.keys())}"
            )

        return reverse_mappings[backend_table_name]

    def get_all_mappings(self) -> dict:
        """Get all current table name mappings.

        Returns:
            Dictionary of all mappings {schema_name: backend_name}
        """
        return self.mappings.copy()

    def add_mapping(self, schema_table_name: str, backend_table_name: str) -> None:
        """Add or update a table name mapping.

        Args:
            schema_table_name: The schema table name
            backend_table_name: The backend table name
        """
        self.mappings[schema_table_name] = backend_table_name

    def remove_mapping(self, schema_table_name: str) -> None:
        """Remove a table name mapping.

        Args:
            schema_table_name: The schema table name to remove

        Raises:
            KeyError: If the schema table name is not found
        """
        if schema_table_name not in self.mappings:
            raise KeyError(f"No mapping found for schema table '{schema_table_name}'")

        del self.mappings[schema_table_name]
