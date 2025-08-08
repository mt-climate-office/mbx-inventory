from typing import Protocol
from enum import Enum


class InventoryBackend(Protocol):
    def read_records(self, table: str) -> list[dict]:
        """Retrieve records from a specified table."""
        ...

    def create_records(self, table: str, records: list[dict]) -> list[dict]:
        """Create new records in a specified table."""
        ...

    def update_records(self, table: str, records: list[dict]) -> list[dict]:
        """Update existing records in a specified table."""
        ...

    def validate(self) -> bool: ...


class Inventory:
    def __init__(self, backend: InventoryBackend):
        self.backend = backend

    def validate(self) -> bool:
        return self.backend.validate()

    def read(self, table: str, **kwargs) -> list[dict]:
        return self.backend.read_records(table, **kwargs)

    def create(self, table: str, records: list[dict], **kwargs) -> list[dict]:
        return self.backend.create_records(table, records, **kwargs)

    def update(self, table: str, records: list[dict], **kwargs) -> list[dict]:
        return self.backend.update_records(table, records, **kwargs)


class Backends(Enum):
    airtable = "airtable"
    nocodb = "nocodb"
    baserow = "baserow"
