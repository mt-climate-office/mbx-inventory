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
    init_columns: list[Column]
    relational_columns: list[Column] = field(default_factory=list)
    table_id: str | None = None

    def build_request_json(self) -> dict[str, Any]:
        self.init_columns.append(
            Column(
                "_id", "ID", extra={
                    "pk": True,
                    "unique": True
                }
            )
        )
        cols = [x.as_dict() for x in self.init_columns]
        return {
            "table_name": self.table_name,
            "columns": cols
        }


# TODO: Make a table list class that is searchable so we can populate with column id's 
# You need to create linked columns first before you link them, then link with their id.

tables = [
    Table(
        "Stations",
        init_columns=[
            Column("station", "SingleLineText"),
            Column("name", "SingleLineText"),
            Column("status", "MultiSelect"),
            Column("date_installed", "Date"),
            Column("location", "GeoData"),
            Column("elevation", "Number"),
            Column("extra", "JSON"),
            Column("Deployments", "SingleLineText"),
            Column("Maintenance", "SingleLineText"),
            Column("Contacts", "SingleLineText"),
        ],
        relational_columns=[
            Column(
                "Contacts", "LinkToAnotherRecord",
                extra={
                    "childId"
                }
            )
        ]
    ),
    Table(
        "Inventory",
        init_columns=[
            Column("serial_number", "SingleLineText")
        ]
    ),
    Table(
        "Deployments",
        init_columns=[
            Column("install_config", "JSON"),
            Column("date_assigned", "Date"),
            Column("date_start", "Date"),
            Column("date_end", "Date")       
        ]
    ),
    Table(
        "Model Elements",
        init_columns=[
            Column("range_min", "Number"),
            Column("range_max", "Number"),
            Column("qc_units", "SingleLineText")
        ]
    ),
    Table(
        "Models",
        init_columns=[
            Column("manufacturer", "MultiSelect"),
            Column("model", "SingleLineText"),
            Column("component_type", "MultiSelect"),
        ]
    ),
    Table(
        "Elements",
        init_columns=[
            Column("element", "SingleLineText"),
            Column("description", "SingleLineText"),
            Column("extra", "JSON")
        ]
    ),
    Table(
        "Measurements",
        init_columns=[
            Column("measurement", "SingleLineText"),
            Column("measured_units", "SingleLineText")
        ]
    ),
    Table(
        "Maintenance",
        init_columns=[
            Column("Created Date", "Date"),
            Column("Visit Date", "Date"),
            Column("End Date", "Date"),
            Column("Task Description", "LongText"),
            Column("Task Comments", "LongText"),
            Column("Trip Type", "MultiSelect"),
            Column("Status", "MultiSelect")
        ]
    ),
    Table(
        "Outages",
        init_columns=[
            Column("outage_start", "Date"),
            Column("outage_end", "Date")
        ]
    ),
    Table(
        "Contacts",
        init_columns=[
            Column("name_first", "SingleLineText"),
            Column("name_last", "SingleLineText"),
            Column("phone_number", "PhoneNumber"),
            Column("email", "Email"),
            Column("station", "SingleLineText"),
        ]
    )

]