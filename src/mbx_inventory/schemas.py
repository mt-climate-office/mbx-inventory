from dataclasses import dataclass, field, asdict
from typing import Any
from pathlib import Path
import json


@dataclass
class Column:
    column_name: str
    uidt: str
    name: str = field(init=False)
    extra: dict = field(default_factory=dict)
    column_id: str | None = None
    is_primary: bool = False

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

    def __getitem__(self, key: str) -> Column:
        for column in self.columns:
            if column.column_name == key:
                return column

        # TODO: Figure out how to put foreignly created relationships here.
        for relationship in self.relationships:
            if relationship.column_name == key:
                return relationship

        for lookup in self.lookups:
            if lookup.column_name == key:
                return lookup

        raise KeyError(f"Column with name {key} is not present in {self.table_name}.")


@dataclass
class BaseSchema:
    base_id: str
    tables: list[Table]

    def __getitem__(self, key: str) -> Table:
        for table in self.tables:
            if table.table_name == key:
                return table

        raise KeyError(f"Table with name '{key}' is not in list of tables.")

    def match_relationship_column_ids(self) -> None:
        for table in self.tables:
            for relationship in table.relationships:
                child = relationship.extra["childId"]
                relationship.extra["childId"] = self[child].table_id
                relationship.extra["parentId"] = table.table_id

    def match_lookup_column_ids(self) -> None:
        for table in self.tables:
            for lookup in table.lookups:
                target = lookup.extra["fk_relation_column_id"]
                target = table[target]

                lookup.extra["fk_relation_column_id"] = target.table_id
                lookup.extra["fk_lookup_column_id"] = target[
                    lookup.extra["fk_lookup_column_id"]
                ].column_id

    def save(self, pth: Path) -> None:
        out = asdict(self)
        with open(pth, "w") as json_file:
            json.dump(out, json_file, indent=4)


TABLES = [
    Table(
        "Stations",
        columns=[
            Column("station", "SingleLineText", is_primary=True),
            Column("name", "SingleLineText"),
            Column("status", "MultiSelect"),
            Column("date_installed", "Date"),
            Column("location", "GeoData"),
            Column("elevation", "Number"),
            Column("extra", "JSON"),
        ],
        relationships=[
            Column(
                "Contacts",
                "Links",
                extra={
                    "childId": "Contacts",
                    "type": "mm",
                    "title": "Contacts",
                },
            ),
            Column(
                "Deployments",
                "Links",
                extra={
                    "childId": "Deployments",
                    "type": "hm",
                    "title": "Deployments",
                },
            ),
            Column(
                "Maintenance",
                "Links",
                extra={"childId": "Maintenance", "type": "mm", "title": "Maintenance"},
            ),
        ],
    ),
    Table(
        "Inventory",
        columns=[
            Column("serial_number", "SingleLineText"),
            Column("extra", "JSON"),
        ],
        relationships=[
            Column(
                "Deployments",
                "Links",
                extra={
                    "childId": "Deployments",
                    "type": "hm",
                    "title": "Deployments",
                },
            ),
            Column(
                "Maintenance",
                "Links",
                extra={
                    "childId": "Maintenance",
                    "type": "mm",
                    "title": "Maintenance",
                },
            ),
        ],
        lookups=[
            Column(
                "manufacturer",
                "Lookup",
                extra={
                    # TODO: This one is id in target table
                    "fk_lookup_column_id": "manufacturer",
                    # TODO: This one is the column in the current table
                    "fk_relation_column_id": "Models",
                    "title": "manufacturer",
                },
            ),
            Column(
                "model",
                "Lookup",
                extra={
                    "fk_lookup_column_id": "model",
                    "fk_relation_column_id": "Models",
                    "title": "model",
                },
            ),
            Column(
                "component_type",
                "Lookup",
                extra={
                    "fk_lookup_column_id": "component_type",
                    "fk_relation_column_id": "Models",
                    "title": "component_type",
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
                "Inventory",
                "LinkToAnotherRecord",
                extra={"childId": "Inventory", "type": "hm", "title": "Inventory"},
            ),
            Column(
                "Model Elements",
                "Links",
                extra={
                    "childId": "Model Elements",
                    "type": "hm",
                    "title": "Model Elements",
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
                "Model Elements",
                "Links",
                extra={
                    "childId": "Model Elements",
                    "type": "hm",
                    "title": "Model Elements",
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
                "Model Elements",
                "Links",
                extra={
                    "childId": "Model Elements",
                    "type": "hm",
                    "title": "Model Elements",
                },
            ),
            Column(
                "Elements",
                "Links",
                extra={
                    "childId": "Elements",
                    "type": "hm",
                    "title": "Elements",
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
