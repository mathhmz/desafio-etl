from dagster import AssetSelection, Definitions, ScheduleDefinition, asset_check, define_asset_job, load_assets_from_modules

from . import assets
from . import asset_checks
from .io.postgres_io_manager import postgres_pandas_io_manager
all_assets = load_assets_from_modules([assets])

all_asset_checks = [asset_checks.check_proposicoes_bronze_data_format, asset_checks.check_proposicoes_digest_format, asset_checks.check_proposicoes_raw_data_format, asset_checks.check_proposicoes_silver_format, asset_checks.check_tramitacoes_bronze_data_format, asset_checks.check_tramitacoes_digest_format, asset_checks.check_tramitacoes_silver_format]

etl_job = define_asset_job("etl_job", selection=AssetSelection.all())




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
        "silver_io_manager": silver_io_manager },
    jobs= [etl_job],
    asset_checks=all_asset_checks,
    
)

etl_schedule = ScheduleDefinition(
    job=etl_job,
    cron_schedule="0 3 * * *",  # every day at 3 AM
)
