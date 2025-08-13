"""
NetworkSchemaMixin for adding network schema-specific methods to inventory classes.

This module provides the NetworkSchemaMixin class that adds specialized methods
for retrieving network schema data from inventory backends (primarily AirTable).
"""

from typing import Dict, List, Any, Optional, TYPE_CHECKING
import logging

from .transformers import (
    ElementsTransformer,
    StationsTransformer,
    ComponentModelsTransformer,
    InventoryTransformer,
    DeploymentsTransformer,
    ComponentElementsTransformer,
    RequestSchemasTransformer,
    ResponseSchemasTransformer,
    TableNameMapper,
    BackendError,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class NetworkSchemaMixin:
    """Mixin providing network schema-specific methods for inventory backends.

    This mixin adds specialized methods for retrieving data from inventory backends
    that correspond to network schema tables. It handles data transformation,
    validation, and error handling for each network schema table type.

    The mixin is designed to be used with the Inventory class to provide both
    generic CRUD operations and network schema-specific methods.
    """

    def __init__(
        self,
        *args,
        table_mappings: Optional[Dict[str, str]] = None,
        table_configs: Optional[Dict[str, Any]] = None,
        backend_type: str = "airtable",
        **kwargs,
    ):
        """Initialize the NetworkSchemaMixin.

        Args:
            *args: Arguments passed to parent class
            table_mappings: Optional custom table name mappings
            table_configs: Optional per-table configuration with field mappings
            backend_type: Type of backend ("airtable", "baserow", "nocodb")
            **kwargs: Keyword arguments passed to parent class
        """
        super().__init__(*args, **kwargs)

        # Store table configs for field mapping
        self.table_configs = table_configs or {}

        # Initialize table name mapper
        self.table_mapper = TableNameMapper(
            custom_mappings=table_mappings, backend_type=backend_type
        )

        # Store backend type for reference
        self.backend_type = backend_type.lower()

    def _get_backend_data(
        self, schema_table_name: str, **kwargs
    ) -> List[Dict[str, Any]]:
        """Get raw data from the backend for a given schema table.

        Args:
            schema_table_name: The schema table name (e.g., "elements")
            **kwargs: Additional arguments passed to backend read method

        Returns:
            List of raw records from the backend

        Raises:
            BackendError: If backend operation fails
        """
        try:
            # Get the backend table name
            backend_table_name = self.table_mapper.get_backend_table_name(
                schema_table_name
            )

            # Call the backend's read method
            # Note: self.backend should be available from the parent Inventory class
            raw_data = self.backend.read_records(backend_table_name, **kwargs)

            logger.debug(f"Retrieved {len(raw_data)} records from {backend_table_name}")
            return raw_data

        except KeyError as e:
            raise BackendError(f"Table mapping error: {e}") from e
        except Exception as e:
            raise BackendError(f"Failed to retrieve data from backend: {e}") from e

    def _transform_data(
        self, transformer_class, raw_data: List[Dict[str, Any]], table_name: str
    ) -> List[Dict[str, Any]]:
        """Transform raw backend data using the specified transformer.

        Args:
            transformer_class: The transformer class to use
            raw_data: Raw data from the backend
            table_name: Name of the table being transformed

        Returns:
            Transformed data ready for database insertion

        Raises:
            TransformationError: If data transformation fails
        """
        try:
            # Check if we have custom field mappings for this table
            if table_name in self.table_configs and hasattr(
                self.table_configs[table_name], "field_mappings"
            ):
                new_mappings = self.table_configs[table_name].field_mappings
                transformer_class.override_field_mappings(field_mappings=new_mappings)

            return transformer_class.transform(raw_data)
        except Exception as e:
            logger.error(
                f"Data transformation failed with {transformer_class.__name__}: {e}"
            )
            raise

    def _apply_filters(
        self, data: List[Dict[str, Any]], filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Apply filters to the transformed data.

        Args:
            data: Transformed data
            filters: Optional filters to apply

        Returns:
            Filtered data
        """
        if not filters:
            return data

        filtered_data = []
        for record in data:
            include_record = True

            for field, value in filters.items():
                if field not in record:
                    include_record = False
                    break

                # Handle different filter types
                if isinstance(value, list):
                    # Multiple values (OR condition)
                    if record[field] not in value:
                        include_record = False
                        break
                elif isinstance(value, dict):
                    # Complex filters (e.g., range, contains)
                    if not self._evaluate_complex_filter(record[field], value):
                        include_record = False
                        break
                else:
                    # Exact match
                    if record[field] != value:
                        include_record = False
                        break

            if include_record:
                filtered_data.append(record)

        return filtered_data

    def _evaluate_complex_filter(
        self, field_value: Any, filter_spec: Dict[str, Any]
    ) -> bool:
        """Evaluate complex filter specifications.

        Args:
            field_value: The field value to test
            filter_spec: Filter specification (e.g., {"min": 10, "max": 100})

        Returns:
            True if the field value matches the filter specification
        """
        for operator, expected_value in filter_spec.items():
            if operator == "min" and field_value < expected_value:
                return False
            elif operator == "max" and field_value > expected_value:
                return False
            elif operator == "contains" and expected_value not in str(field_value):
                return False
            elif operator == "startswith" and not str(field_value).startswith(
                str(expected_value)
            ):
                return False
            elif operator == "endswith" and not str(field_value).endswith(
                str(expected_value)
            ):
                return False

        return True

    # Network Schema Methods

    # Mapping of table names to their transformers
    _TRANSFORMER_MAP = {
        "elements": ElementsTransformer,
        "component_models": ComponentModelsTransformer,
        "stations": StationsTransformer,
        "inventory": InventoryTransformer,
        "deployments": DeploymentsTransformer,
        "component_elements": ComponentElementsTransformer,
        "request_schemas": RequestSchemasTransformer,
        "response_schemas": ResponseSchemasTransformer,
    }

    def _get_network_data(
        self, table_name: str, filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Generic method to retrieve network schema data from inventory backend.

        Args:
            table_name: Name of the table to retrieve data from
            filters: Optional filters to apply to the data

        Returns:
            List of data formatted for network schema

        Raises:
            BackendError: If backend operation fails
            TransformationError: If data transformation fails
            ValueError: If table_name is not supported
        """
        if table_name not in self._TRANSFORMER_MAP:
            raise ValueError(f"Unsupported table name: {table_name}")

        try:
            # Get raw data from backend
            raw_data = self._get_backend_data(table_name)

            # Transform data using appropriate transformer
            transformer_class = self._TRANSFORMER_MAP[table_name]
            transformed_data = self._transform_data(
                transformer_class, raw_data, table_name
            )

            # Apply filters if provided
            filtered_data = self._apply_filters(transformed_data, filters)

            logger.info(f"Retrieved {len(filtered_data)} {table_name} records")
            return filtered_data

        except Exception as e:
            logger.error(f"Failed to get {table_name} data: {e}")
            raise

    # Convenience methods that delegate to the generic method
    def get_elements(
        self, filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        return self._get_network_data("elements", filters)

    def get_component_models(
        self, filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        return self._get_network_data("component_models", filters)

    def get_stations(
        self, filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        return self._get_network_data("stations", filters)

    def get_inventory(
        self, filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        return self._get_network_data("inventory", filters)

    def get_deployments(
        self, filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        return self._get_network_data("deployments", filters)

    def get_component_elements(
        self, filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        return self._get_network_data("component_elements", filters)

    def get_request_schemas(
        self, filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        return self._get_network_data("request_schemas", filters)

    def get_response_schemas(
        self, filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        return self._get_network_data("response_schemas", filters)
