from dagster import Definitions, load_assets_from_modules

from . import assets
from .io.postgres_io_manager import postgres_pandas_io_manager

all_assets = load_assets_from_modules([assets])



silver_io_manager = postgres_pandas_io_manager.configured(
    {
        'host': {'env': 'SILVER_DB_HOST'},
        'database': {'env': 'SILVER_DB_NAME'},
        'user': {'env': 'SILVER_DB_USER'},
        'password': {'env': 'SILVER_DB_PASSWORD'},
        'port': {'env': 'SILVER_DB_PORT'},
    }
)

defs = Definitions(
    assets=all_assets,
    resources={
        "silver_io_manager": silver_io_manager }
)
