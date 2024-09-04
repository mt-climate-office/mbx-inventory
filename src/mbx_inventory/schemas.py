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
        if "extra" in items:
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
        self.columns.append(Column("_id", "ID", extra={"pk": True, "unique": True}))
        cols = [x.as_dict() for x in self.columns]
        return {"table_name": self.table_name, "columns": cols}

    def __getitem__(self, key):
        for column in self.columns:
            if column.column_name == key:
                return column
        raise KeyError(f"Column with name {key} is not present in {self.table_name}.")


@dataclass
class BaseSchema:
    tables: list[Table]

    def __getitem__(self, key: str) -> Table:
        for table in self.tables:
            if table.table_name == key:
                return table

        raise KeyError(f"Table with name '{key}' is not in list of tables.")

    # def search_tables_for_relationship(self, table_name: str, column_name: str) -> str:
    #     for table in self.tables:
    #         if table.table_name == table_name:
    #             break
    #     for column in table.columns:
    #         if column.column_name == column_name:
    #             break

    #     if column_name != column.column_name:
    #         raise ValueError(
    #             f"Couldn't find a match for table {table_name} and column {column_name}."
    #         )

    #     return column.column_id

    def match_relationship_column_ids(self) -> None:
        for table in self.tables:
            for relationship in table.relationships:
                child = relationship.extra["childId"]
                relationship.extra["childId"] = self[child].table_id
                relationship.extra["parentId"] = table.table_id


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
            Column("maintenance", "SingleLineText"),
        ],
        relationships=[
            Column(
                "contacts",
                "Links",
                extra={
                    "childId": "Contacts",
                    "type": "mm",
                    "title": "contacts",
                },
            ),
            Column(
                "deployments",
                "Links",
                extra={
                    "childId": "Deployments",
                    "type": "hm",
                    "title": "deployments",
                },
            ),
            Column(
                "maintenance",
                "Links",
                extra={"childId": "Maintenance", "type": "mm", "title": "maintenance"},
            ),
        ],
    ),
    Table(
        "Inventory",
        columns=[Column("serial_number", "SingleLineText")],
        relationships=[
            Column(
                "deployments",
                "Links",
                extra={
                    "childId": "Deployments",
                    "type": "hm",
                    "title": "deployments",
                },
            ),
            Column(
                "maintenance",
                "Links",
                extra={
                    "childId": "Maintenance",
                    "type": "mm",
                    "title": "maintenance",
                },
            ),
        ],
    ),
    Table(
        "Deployments",
        columns=[
            Column("install_config", "JSON"),
            Column("date_assigned", "Date"),
            Column("date_start", "Date"),
            Column("date_end", "Date"),
        ],
        relationships=[
            Column(
                "outages",
                "Links",
                extra={"childId": "Outages", "type": "hm", "title": "Outages"},
            ),
            Column(
                "maintenance",
                "Links",
                extra={"childId": "Maintenance", "type": "mm", "title": "Maintenance"},
            ),
        ],
    ),
    Table(
        "Model Elements",
        columns=[
            Column("range_min", "Number"),
            Column("range_max", "Number"),
            Column("qc_units", "SingleLineText"),
        ],
    ),
    Table(
        "Models",
        columns=[
            Column("manufacturer", "MultiSelect"),
            Column("model", "SingleLineText"),
            Column("component_type", "MultiSelect"),
        ],
        relationships=[
            Column(
                "model_id",
                "LinkToAnotherRecord",
                extra={"childId": "Inventory", "type": "hm", "title": "model_id"},
            ),
            Column(
                "model_elements",
                "Links",
                extra={
                    "childId": "Model Elements",
                    "type": "hm",
                    "title": "model_elements",
                },
            ),
        ],
    ),
    Table(
        "Elements",
        columns=[
            Column("element", "SingleLineText"),
            Column("description", "SingleLineText"),
            Column("extra", "JSON"),
        ],
        relationships=[
            Column(
                "element_rec",
                "Links",
                extra={
                    "childId": "Model Elements",
                    "type": "hm",
                    "title": "element_rec",
                },
            ),
        ],
    ),
    Table(
        "Measurements",
        columns=[
            Column("measurement", "SingleLineText"),
            Column("measured_units", "SingleLineText"),
        ],
        relationships=[
            Column(
                "model_elements",
                "Links",
                extra={
                    "childId": "Model Elements",
                    "type": "hm",
                    "title": "model_elements",
                },
            ),
            Column(
                "measurement_rec",
                "Links",
                extra={
                    "childId": "Elements",
                    "type": "hm",
                    "title": "measurement_rec",
                },
            ),
        ],
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
            Column("status", "MultiSelect"),
        ],
    ),
    Table(
        "Outages",
        columns=[Column("outage_start", "Date"), Column("outage_end", "Date")],
    ),
    Table(
        "Contacts",
        columns=[
            Column("name_first", "SingleLineText"),
            Column("name_last", "SingleLineText"),
            Column("phone_number", "PhoneNumber"),
            Column("email", "Email"),
            Column("station", "SingleLineText"),
        ],
    ),
]
