from pyairtable import Api


class AirtableBackend:
    def __init__(self, api_key: str, base_id: str):
        self.api = Api(api_key)
        self.base = self.api.base(base_id)

    def validate(self) -> bool:
        """Check if the API token is valid by attempting to access base metadata."""
        try:
            self.base.schema()
            return True
        except Exception:
            return False

    def read_records(self, table: str) -> list[dict]:
        """Retrieve records from a specified table."""
        table_obj = self.base.table(table)
        records = table_obj.all()
        
        recs = [{"inventory_id": record["id"], **record["fields"]} for record in records]
        return [{k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in x.items()} for x in recs]

    def create_records(self, table: str, records: list[dict]) -> list[dict]:
        """Create new records in a specified table."""
        table_obj = self.base.table(table)

        # Format records for pyairtable (exclude id if present)
        fields_only = [
            {k: v for k, v in record.items() if k != "id"} for record in records
        ]

        created_records = table_obj.batch_create(fields_only)
        return [{"id": record["id"], **record["fields"]} for record in created_records]

    def update_records(self, table: str, records: list[dict]) -> list[dict]:
        """Update existing records in a specified table."""
        table_obj = self.base.table(table)

        # Format records for pyairtable (id and fields separate)
        formatted_records = [
            {
                "id": record["id"],
                "fields": {k: v for k, v in record.items() if k != "id"},
            }
            for record in records
        ]

        updated_records = table_obj.batch_update(formatted_records)
        return [{"id": record["id"], **record["fields"]} for record in updated_records]
