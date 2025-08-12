from typing import Protocol, Dict, Optional
from enum import Enum

from .network_schema_mixin import NetworkSchemaMixin


class InventoryBackend(Protocol):
    def read_records(self, table: str, **kwargs) -> list[dict]:
        """Retrieve records from a specified table."""
        ...

    def create_records(self, table: str, records: list[dict], **kwargs) -> list[dict]:
        """Create new records in a specified table."""
        ...

    def update_records(self, table: str, records: list[dict], **kwargs) -> list[dict]:
        """Update existing records in a specified table."""
        ...

    def validate(self) -> bool: ...


class Inventory(NetworkSchemaMixin):
    """Inventory management class with both generic CRUD and network schema methods.

    This class provides:
    1. Generic CRUD operations (read, create, update) for any table
    2. Network schema-specific methods (get_elements, get_stations, etc.)
    3. Data transformation and validation for network schema tables
    4. Support for multiple inventory backends (AirTable, Baserow, NocoDB)
    """

    def __init__(
        self,
        backend: InventoryBackend,
        table_mappings: Optional[Dict[str, str]] = None,
        backend_type: str = "airtable",
    ):
        """Initialize the Inventory instance.

        Args:
            backend: The inventory backend implementation
            table_mappings: Optional custom table name mappings
            backend_type: Type of backend ("airtable", "baserow", "nocodb")
        """
        self.backend = backend

        # Initialize the NetworkSchemaMixin
        super().__init__(table_mappings=table_mappings, backend_type=backend_type)

    def validate(self) -> bool:
        """Validate the backend connection.

        Returns:
            True if backend is valid and accessible
        """
        return self.backend.validate()

    def read(self, table: str, **kwargs) -> list[dict]:
        """Generic method to read records from any table.

        Args:
            table: Table name to read from
            **kwargs: Additional arguments passed to backend

        Returns:
            List of raw records from the backend
        """
        return self.backend.read_records(table, **kwargs)

    def create(self, table: str, records: list[dict], **kwargs) -> list[dict]:
        """Generic method to create records in any table.

        Args:
            table: Table name to create records in
            records: List of records to create
            **kwargs: Additional arguments passed to backend

        Returns:
            List of created records from the backend
        """
        return self.backend.create_records(table, records, **kwargs)

    def update(self, table: str, records: list[dict], **kwargs) -> list[dict]:
        """Generic method to update records in any table.

        Args:
            table: Table name to update records in
            records: List of records to update
            **kwargs: Additional arguments passed to backend

        Returns:
            List of updated records from the backend
        """
        return self.backend.update_records(table, records, **kwargs)


class Backends(Enum):
    airtable = "airtable"
    nocodb = "nocodb"
    baserow = "baserow"
