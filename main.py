from mbx_inventory import create_db_schema as create
from mbx_inventory.schemas import tables, BaseSchema
from dotenv import load_dotenv
import os

load_dotenv()


api_key = os.getenv("xc-token")
base_id = create.create_mesonet_base(api_key=api_key, db_base_name="Montana Mesonet")

tables = create.create_base_tables(tables, base_id, api_key)
base_schema = BaseSchema(tables)
base_schema.match_relationship_column_ids()
base_schema = create.populate_table_relationships(
    base_schema=base_schema, api_key=api_key
)
pass
