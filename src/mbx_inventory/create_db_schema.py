import httpx

TABLES = {
    "Stations": {
        "init_columns": [
            {
                "column_name": "_id",
                "title": "_id",
                "uidt": "ID",
                "pk": True,
                "unique": True
            },
            {
                "column_name": "station",
                "title": "station",
                "uidt": "SingleLineText"
            },
            {
                "column_name": "name",
                "title": "name",
                "uidt": "SingleLineText"
            },
            {
                "column_name": "status",
                "title": "status",
                "uidt": "MultiSelect"
            },
            {
                "column_name": "date_installed",
                "title": "date_installed",
                "uidt": "Date",
            },
            {
                "column_name": "location",
                "title": "location",
                "uidt": "GeoData",
            },
            {
                "column_name": "elevation",
                "title": "elevation",
                "uidt": "Number",
            },
            # {
            #     "column_name": "_id",
            #     "title": "_id",
            #     "uidt": "Formula",
            #     "formula_raw": "concat(field('name'), ': ', field('station'))"
            # },
            {
                "column_name": "extra",
                "title": "extra",
                "uidt": "JSON",
            }
        ],
        "relational_columns": [],
    },
    "Inventory": {
        "init_columns": [
            {
                "column_name": "serial_number",
                "title": "serial_number",
                "uidt": "SingleLineText",
            },
        ],
        "relational_columns": []
    },
    "Deployments": {
        "init_columns": [
            {
                "column_name": "install_config",
                "title": "install_config",
                "uidt": "JSON"
            },
            {
                "column_name": "date_assigned", 
                "title": "date_assigned",
                "uidt": "Date"
            },
            {
                "column_name": "date_start", 
                "title": "date_start",
                "uidt": "Date"
            },
            {
                "column_name": "date_end", 
                "title": "date_end",
                "uidt": "Date"
            }
        ],
        "relational_columns": []
    },
    "Model Elements": {
        "init_columns": [
            {
                "column_name": "range_min", 
                "title": "range_min",
                "uidt": "Number"
            },
            {
                "column_name": "range_max", 
                "title": "range_max",
                "uidt": "Number"
            },
            {
                "column_name": "qc_units", 
                "title": "qc_units",
                "uidt": "SingleLineText"
            },
        ],
        "relational_columns": []
    },
    "Models": {
        "init_columns": [],
        "relational_columns": []
    },
    "Elements": {
        "init_columns": [],
        "relational_columns": []
    },
    "Measurements": {
        "init_columns": [],
        "relational_columns": []
    },
    "Maintenance": {
        "init_columns": [],
        "relational_columns": []
    },
    "Outages": {
        "init_columns": [],
        "relational_columns": []
    },
    "Contacts": {
        "init_columns": [],
        "relational_columns": []
    },
}


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
) -> dict[str: str]:
    
    table_ids = {}
    for table, columns in TABLES.items():

        resp = httpx.post(
            f"{nocodb_url}/api/v2/meta/bases/{base_id}/tables",
            headers={
                "xc-token": api_key,
                "Content-Type": "application/json"
            },
            json={
                "columns": columns['init_columns'],
                "table_name": table
            }
        )

        resp = check_resp_status_code(resp)
        resp_json = resp.json()
        table_ids[table] = resp_json['id']
    
    return table_ids


base_id = create_mesonet_base(
    api_key=api_key,
    db_base_name = "Montana Mesonet"
)

tables = create_base_tables(base_id, api_key)