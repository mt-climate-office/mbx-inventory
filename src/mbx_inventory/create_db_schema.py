import httpx
from typing import Literal
from .schemas import Table, BaseSchema


def check_resp_status_code(resp) -> httpx.Response:
    if resp.status_code != 200:
        raise httpx.RequestError(f"Error while running {resp.request}:\n{resp.json()}")

    return resp


def get_nocodb_bases(
    api_key: str,
    nocodb_url: str = "http://loclahost:8080",
) -> httpx.Response:
    resp = httpx.get(
        f"{nocodb_url}/api/v2/meta/bases",
        headers={"xc-token": api_key, "Content-Type": "application/json"},
    )

    resp = check_resp_status_code(resp)
    return resp


def create_mesonet_base(
    api_key: str,
    nocodb_url: str = "http://localhost:8080",
    db_base_name: str = "Mesonet",
) -> str:
    resp = httpx.post(
        f"{nocodb_url}/api/v2/meta/bases",
        json={"title": db_base_name, "type": "database", "external": False},
        headers={"xc-token": api_key, "Content-Type": "application/json"},
    )

    resp = check_resp_status_code(resp)
    try:
        return resp.json()["id"]
    except KeyError:
        raise ValueError("No id associated with table. Please try again.")


def create_base_tables(
    tables: list[Table],
    base_id: str,
    api_key: str,
    nocodb_url: str = "http://localhost:8080",
) -> list[Table]:
    for table in tables:
        resp = httpx.post(
            f"{nocodb_url}/api/v2/meta/bases/{base_id}/tables",
            headers={"xc-token": api_key, "Content-Type": "application/json"},
            json=table.build_request_json(),
        )

        resp = check_resp_status_code(resp)
        resp_json = resp.json()
        table.table_id = resp_json["id"]

        for column in table.columns:
            _id = [
                x
                for x in resp_json["columns"]
                if x["column_name"] == column.column_name
            ]
            if len(_id) != 1:
                raise ValueError(
                    f"Incorrect number of columns matching {column.column_name}"
                )
            else:
                _id = _id[0]
            column.column_id = _id["id"]

    return tables


def populate_relationships_lookups_formulas(
    column_type: Literal["relationships", "lookups", "formulas"],
    base_schema: BaseSchema,
    api_key: str,
    nocodb_url: str = "http://localhost:8080",
):
    assert column_type in [
        "relationships",
        "lookups",
        "formulas",
    ], "Column type must be one of ['relationships', 'lookups', 'formulas']."
    for table in base_schema.tables:
        match column_type:
            case "relationships":
                columns = table.relationships
            case "lookups":
                columns = table.lookups
            case "formulas":
                columns = table.formulas

        for col in columns:
            resp = httpx.post(
                f"{nocodb_url}/api/v2/meta/tables/{table.table_id}/columns",
                headers={"xc-token": api_key, "Content-Type": "application/json"},
                json=col.as_dict(),
            )
            resp = check_resp_status_code(resp)
            col.column_id = resp.json()["id"]

    return base_schema


def create_primary_columns(
    base_schema: BaseSchema,
    api_key: str,
    nocodb_url: str = "http://localhost:8080",
):
    for table in base_schema.tables:
        for column in table.columns:
            if column.is_primary:
                resp = httpx.post(
                    f"{nocodb_url}/api/v2/meta/columns/{column.column_id}/primary",
                    headers={"xc-token": api_key, "Content-Type": "application/json"},
                )

                check_resp_status_code(resp)
                break


# TODO: Add lookups and formulas next. Then pinned columns. Then start AT transfer.
