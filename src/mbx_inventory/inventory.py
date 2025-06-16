from typing import Protocol

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


class Inventory:

    def __init__(self, backend: InventoryBackend):
        self.backend = backend

    def read(self, table: str) -> list[dict]:
        return self.backend.read_records(table)

    def create(self, table: str, records: list[dict]) -> list[dict]:
        return self.backend.create_records(table, records)

    def update(self, table: str, records: list[dict]) -> list[dict]:
        return self.backend.update_records(table, records)