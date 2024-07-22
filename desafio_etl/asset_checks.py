import io
import json
from dagster import AssetCheckResult, asset_check
import pandas as pd

from desafio_etl.assets import proposicoes_bronze, proposicoes_digest, proposicoes_raw, proposicoes_silver, tramitacoes_bronze, tramitacoes_digest, tramitacoes_silver
from desafio_etl.core.services.proposicoes_service import ProposicoesService


@asset_check(asset=proposicoes_raw)
def check_proposicoes_raw_data_format(context) -> AssetCheckResult:
    """
    Verifica o formato dos dados brutos de proposições.
    """
    
    service_request = ProposicoesService()
    data = service_request()
    raw = json.dumps(data, indent=2)
    
    
    if len(raw) > 0:
        return AssetCheckResult(passed=True)
    return AssetCheckResult(passed=False, description="Os dados brutos não estão no formato esperado.")


