from mbx_inventory.schemas import BaseSchema, Column
from mbx_inventory.create_db_schema import check_resp_status_code
from mesonet_in_a_box.config import Config
import httpx
from pathlib import Path


base_schema = BaseSchema.load(Path("/home/cbrust/.config/mbx/pzmm1bqzz8iivwu.json"))
CONFIG = Config.load(Config.file)


table_id = 'mllkt0116qzkkzp'
def get_table_records(table_id, params: dict | None=None, content: list | None=None):
    if params is None:
        params = {}
    if content is None: 
        content = []
    
    while True:
        resp = httpx.get(
            f"{CONFIG.nocodb_url}/api/v2/tables/{table_id}/records",
            headers={
                "xc-token": CONFIG.nocodb_token,
                "Content-Type": "application/json",
            },
            params=params
        )

        check_resp_status_code(resp)
        resp = resp.json()
        if len(data := resp.get("list", [])) > 0:
            content.extend(data)

        if not resp['pageInfo'].get('isLastPage', True):
            offset = resp['pageInfo']['pageSize'] * resp['pageInfo'].get("page", 1)
            params['offset'] = offset
        else:
            break
        break

def delete_table(table_id):
    resp = httpx.delete(
        f"{CONFIG.nocodb_url}/api/v2/meta/tables/{table_id}",
        headers={
            "xc-token": CONFIG.nocodb_token,
            "Content-Type": "application/json",
        },
    )
    check_resp_status_code(resp)


def delete_column(column_id):
    resp = httpx.delete(
        f"{CONFIG.nocodb_url}/api/v2/meta/columns/{column_id}",
        headers={
            "xc-token": CONFIG.nocodb_token,
            "Content-Type": "application/json",
        },
    )
    check_resp_status_code(resp)


def create_column(table_id, content):
    resp = httpx.post(
        f"{CONFIG.nocodb_url}/api/v2/meta/tables/{table_id}/columns",
        headers={
            "xc-token": CONFIG.nocodb_token,
            "Content-Type": "application/json",
        },
        json=content,
    )
    check_resp_status_code(resp)
    return resp


def delete_unused_tables(base_schema):
    delete_table(base_schema["Vendors"].table_id)
    delete_table(base_schema["Bulk Inventory"].table_id)


def fix_stations_table(base_schema):
    stations = base_schema["Stations"]
    for column in [
        "Outages",
        "partner_secondary",
        "local_manager",
        "allotment_holder",
        "landowner",
        "troubleshooting_contact",
        "access_contact",
        "billing_agreements",
        "data_transfer",
        "secondary_contact",
    ]:
        delete_column(stations[column].column_id)

    resp = httpx.post(
        f"{CONFIG.nocodb_url}/api/v2/meta/tables/{stations.table_id}/columns",
        headers={
            "xc-token": CONFIG.nocodb_token,
            "Content-Type": "application/json",
        },
        json=Column(
            "id_col",
            "Formula",
            extra={
                "formula_raw": "CONCAT({name}, ': ', {station})",
                "title": "id_col",
            },
        ).as_dict(),
    )

    target = [x for x in resp.json()["columns"] if x["column_name"] == "id_col"]
    assert len(target) == 1

    resp = httpx.post(
        f"{CONFIG.nocodb_url}/api/v2/meta/columns/{target[0]['id']}/primary",
        headers={
            "xc-token": CONFIG.nocodb_token,
            "Content-Type": "application/json",
        },
    )

    delete_column(stations["id_1"].column_id)
    create_column(stations.table_id, Column("location", "GeoData").as_dict())
    # To populate, use string that is lat;lng
