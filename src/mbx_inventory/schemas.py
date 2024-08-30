from dataclasses import dataclass, field, asdict
from typing import Any

@dataclass
class Column:
    column_name: str
    uidt: str
    name: str = field(init=False)
    extra: dict = field(default_factory=dict)
    column_id: str | None = None

    def __post_init__(self):
        self.name = self.column_name
    
    def as_dict(self):
        d = asdict(self)
        items = {k: v for k, v in d.items() if v}
        if 'extra' in items:
            extras = items.pop("extra")
            items = {**items, **extras}
        return items


@dataclass
class Table:
    table_name: str
    columns: list[Column]
    relationships: list[Column] = field(default_factory=list)
    lookups: list[Column] = field(default_factory=list)
    formulas: list[Column] = field(default_factory=list)
    table_id: str | None = None

    def build_request_json(self) -> dict[str, Any]:
        self.columns.append(
            Column(
                "_id", "ID", extra={
                    "pk": True,
                    "unique": True
                }
            )
        )
        cols = [x.as_dict() for x in self.columns]
        return {
            "table_name": self.table_name,
            "columns": cols
        }


@dataclass
class BaseSchema:
    tables: list[Table]

    def search_tables_for_relationship(self, table_name: str, column_name: str):
        for table in self.tables:
            if table.table_name == table_name:
                break
        for column in table.columns:
            if column.column_name == column_name:
                break
        
        if column_name != column.column_name:
            raise ValueError(
                f"Couldn't find a match for table {table_name} and column {column_name}."
            )

        return column.column_id

    def match_relationship_column_ids(self):
        for table in self.tables:
            for relationship in table.relationships:
                table, column = relationship.extra['childId']
                relationship.extra['childId'] = self.search_tables_for_relationship(table, column)

                table, column = relationship.extra['parentId']
                relationship.extra['parentId'] = self.search_tables_for_relationship(table, column)

# TODO: Make a table list class that is searchable so we can populate with column id's 
# You need to create linked columns first before you link them, then link with their id.

tables = [
    Table(
        "Stations",
        columns=[
            Column("station", "SingleLineText"),
            Column("name", "SingleLineText"),
            Column("status", "MultiSelect"),
            Column("date_installed", "Date"),
            Column("location", "GeoData"),
            Column("elevation", "Number"),
            Column("extra", "JSON"),
            Column("deployments", "SingleLineText"),
            Column("maintenance", "SingleLineText"),
            Column("contacts", "SingleLineText"),
        ],
        relationships=[
            Column(
                "contacts", "LinkToAnotherRecord",
                extra={
                    "childId": ("Stations", "contacts"),
                    "parentId": ("Contacts", "station"),
                    "type": "hm"
                }
            )
        ]
    ),
    Table(
        "Inventory",
        columns=[
            Column("serial_number", "SingleLineText")
        ]
    ),
    Table(
        "Deployments",
        columns=[
            Column("install_config", "JSON"),
            Column("date_assigned", "Date"),
            Column("date_start", "Date"),
            Column("date_end", "Date")       
        ]
    ),
    Table(
        "Model Elements",
        columns=[
            Column("range_min", "Number"),
            Column("range_max", "Number"),
            Column("qc_units", "SingleLineText")
        ]
    ),
    Table(
        "Models",
        columns=[
            Column("manufacturer", "MultiSelect"),
            Column("model", "SingleLineText"),
            Column("component_type", "MultiSelect"),
        ]
    ),
    Table(
        "Elements",
        columns=[
            Column("element", "SingleLineText"),
            Column("description", "SingleLineText"),
            Column("extra", "JSON")
        ]
    ),
    Table(
        "Measurements",
        columns=[
            Column("measurement", "SingleLineText"),
            Column("measured_units", "SingleLineText")
        ]
    ),
    Table(
        "Maintenance",
        columns=[
            Column("created_date", "Date"),
            Column("visit_date", "Date"),
            Column("end_date", "Date"),
            Column("task_description", "LongText"),
            Column("task_comments", "LongText"),
            Column("trip_type", "MultiSelect"),
            Column("status", "MultiSelect")
        ]
    ),
    Table(
        "Outages",
        columns=[
            Column("outage_start", "Date"),
            Column("outage_end", "Date")
        ]
    ),
    Table(
        "Contacts",
        columns=[
            Column("name_first", "SingleLineText"),
            Column("name_last", "SingleLineText"),
            Column("phone_number", "PhoneNumber"),
            Column("email", "Email"),
            Column("station", "SingleLineText"),
        ]
    )

]