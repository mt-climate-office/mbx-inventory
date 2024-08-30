import httpx
import os
from schemas import tables, Table, BaseSchema

def check_resp_status_code(resp) -> httpx.Response:
    if resp.status_code != 200:
        raise httpx.RequestError(
            f"Error while running {resp.request}:\n{resp.json()}"
        )
    
    return resp


def create_mesonet_base(
    api_key: str,
    nocodb_url: str="http://localhost:8080", 
    db_base_name: str="Mesonet",
) -> str:
    
    resp = httpx.post(
        f"{nocodb_url}/api/v2/meta/bases",
        json={
            "title": db_base_name,
            "type": "database",
            "external": False
        },
        headers={
            "xc-token": api_key,
            "Content-Type": "application/json"
        }
    )

    resp = check_resp_status_code(resp)
    try:
        return resp.json()['id']
    except KeyError:
        raise ValueError("No id associated with table. Please try again.")
    

def create_base_tables(
    base_id: str,
    api_key: str,
    nocodb_url: str="http://localhost:8080",
) -> list[Table]:
    
    for table in tables:

        resp = httpx.post(
            f"{nocodb_url}/api/v2/meta/bases/{base_id}/tables",
            headers={
                "xc-token": api_key,
                "Content-Type": "application/json"
            },
            json=table.build_request_json()
        )

        resp = check_resp_status_code(resp)
        resp_json = resp.json()
        table.table_id = resp_json['id']

        for column in table.columns:
            _id = [x for x in resp_json['columns'] if x['column_name'] == column.column_name]
            if len(_id) != 1:
                raise ValueError(f"Incorrect number of columns matching {column.column_name}")
            else:
                _id = _id[0]
            column.column_id = _id['id']
    
    return tables


def populate_table_relationships(
        base_id: str,
        api_key: str,
        nocodb_url: str="http://localhost:8080",
):
    pass

# api_key = os.getenv("NOCO_TOKEN")
api_key = "PeM67GqXJ7VbPW4RA2WSyZZnwfjfQNQ364_B02iy"
base_id = create_mesonet_base(
    api_key=api_key,
    db_base_name = "Montana Mesonet"
)

tables = create_base_tables(base_id, api_key)
base_schema = BaseSchema(tables)
base_schema.match_relationship_column_ids()
#TODO: Make API call to update relationships after calling