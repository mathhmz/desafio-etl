import io
import json
from dagster import AssetCheckResult, asset_check
import pandas as pd

from desafio_etl.assets.assets import proposicoes_bronze, proposicoes_digest, proposicoes_raw, proposicoes_silver, tramitacoes_bronze, tramitacoes_digest, tramitacoes_silver


@asset_check(asset=proposicoes_raw)
def check_proposicoes_raw_data_format(context, proposicoes_raw) -> AssetCheckResult:
    """
    Verifica o formato dos dados brutos de proposições.
    """
    data = json.loads(proposicoes_raw)
    if isinstance(data, list) and all(isinstance(item, dict) for item in data):
        return AssetCheckResult(passed=True)
    return AssetCheckResult(passed=False, description="Os dados brutos não estão no formato esperado.")

@asset_check(asset=proposicoes_bronze)
def check_proposicoes_bronze_data_format(context, proposicoes_bronze) -> AssetCheckResult:
    """
    Verifica o formato dos dados processados para proposicoes_bronze.
    """
    data = json.loads(proposicoes_bronze)
    df = pd.DataFrame(data)
    expected_columns = ["id", "number", "author", "year", "presentationDate", "ementa", "regime", "situation", "propositionType"]
    if all(col in df.columns for col in expected_columns):
        return AssetCheckResult(passed=True)
    return AssetCheckResult(passed=False, description="Os dados processados para proposicoes_bronze estão faltando colunas esperadas.")

@asset_check(asset=tramitacoes_bronze)
def check_tramitacoes_bronze_data_format(context, tramitacoes_bronze) -> AssetCheckResult:
    """
    Verifica o formato dos dados processados para tramitacoes_bronze.
    """
    data = json.loads(tramitacoes_bronze)
    expected_keys = ["createdAt", "description", "local", "propositionId"]
    if all(all(key in item for key in expected_keys) for item in data):
        return AssetCheckResult(passed=True)
    return AssetCheckResult(passed=False, description="Os dados processados para tramitacoes_bronze estão faltando chaves esperadas.")

@asset_check(asset=tramitacoes_digest)
def check_tramitacoes_digest_format(context, tramitacoes_digest) -> AssetCheckResult:
    """
    Verifica o formato dos dados processados para tramitacoes_digest.
    """
    pq_file = io.BytesIO(tramitacoes_digest)
    df = pd.read_parquet(pq_file)
    expected_columns = ["createdAt", "description", "local"]
    if all(col in df.columns for col in expected_columns):
        return AssetCheckResult(passed=True)
    return AssetCheckResult(passed=False, description="Os dados processados para tramitacoes_digest estão faltando colunas esperadas.")

@asset_check(asset=proposicoes_digest)
def check_proposicoes_digest_format(context, proposicoes_digest) -> AssetCheckResult:
    """
    Verifica o formato dos dados processados para proposicoes_digest.
    """
    pq_file = io.BytesIO(proposicoes_digest)
    df = pd.read_parquet(pq_file)
    expected_columns = ["number", "author", "year", "presentationDate", "ementa", "regime", "situation", "propositionType", "city", "state"]
    if all(col in df.columns for col in expected_columns):
        return AssetCheckResult(passed=True)
    return AssetCheckResult(passed=False, description="Os dados processados para proposicoes_digest estão faltando colunas esperadas.")

@asset_check(asset=proposicoes_silver)
def check_proposicoes_silver_format(context, proposicoes_silver) -> AssetCheckResult:
    """
    Verifica o formato dos dados normalizados para proposicoes_silver.
    """
    df = proposicoes_silver
    expected_columns = ["number", "author", "year", "presentationDate", "ementa", "regime", "situation", "propositionType", "city", "state"]
    if all(col in df.columns for col in expected_columns):
        return AssetCheckResult(passed=True)
    return AssetCheckResult(passed=False, description="Os dados normalizados para proposicoes_silver estão faltando colunas esperadas.")

@asset_check(asset=tramitacoes_silver)
def check_tramitacoes_silver_format(context, tramitacoes_silver) -> AssetCheckResult:
    """
    Verifica o formato dos dados normalizados para tramitacoes_silver.
    """
    df = tramitacoes_silver
    expected_columns = ["createdAt", "description", "local"]
    if all(col in df.columns for col in expected_columns):
        return AssetCheckResult(passed=True)
    return AssetCheckResult(passed=False, description="Os dados normalizados para tramitacoes_silver estão faltando colunas esperadas.")
