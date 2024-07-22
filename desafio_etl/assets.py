from typing import Any
from dagster import (AssetExecutionContext, asset, Output, MetadataValue, AssetCheckResult)
from .core.services.proposicoes_service import ProposicoesService
from .utils.dataframe_normalize import df_normalizer
import json
import pandas as pd
import io

@asset(io_manager_key="io_manager")
def proposicoes_raw(context: AssetExecutionContext) -> Output:
    """
    Obtém dados brutos de proposições e retorna em formato JSON.

    Args:
        context (AssetExecutionContext): O contexto da execução do asset.

    Returns:
        Output: Dados brutos de proposições em formato JSON.
    """
    service_request = ProposicoesService()
    data = service_request()
    raw = json.dumps(data, indent=2)

    return Output(value=raw, metadata={
        "num_rows": len(data)
    })
    
@asset
def proposicoes_bronze(context: AssetExecutionContext, proposicoes_raw: str) -> Output:
    """
    Processa dados brutos de proposições e transforma em um DataFrame.

    Args:
        context (AssetExecutionContext): O contexto da execução do asset.
        proposicoes_raw (str): Dados brutos de proposições em formato JSON.

    Returns:
        Output: Dados de proposições processados em formato JSON.
    """
    data = json.loads(proposicoes_raw)
    lista_itens = [item['resultado']['listaItem'] for item in data if 'resultado' in item and 'listaItem' in item['resultado']]

    columns = {
        "index": "id",
        "numero": "number",
        "autor": "author",
        "ano": "year",
        "dataPublicacao": "presentationDate",
        "ementa": "ementa",
        "regime": "regime",
        "situacao": "situation",
        "siglaTipoProjeto": "propositionType",
        "listaHistoricoTramitacoes": "listaHistoricoTramitacoes"
    }

    lista_itens_filtrada = [{k: v for k, v in item.items() if k in columns} for items in lista_itens for item in items]
    df = pd.DataFrame(lista_itens_filtrada).rename(columns=columns)
    df = df.reset_index()
    df['id'] = df['id'].astype(int) + 1

    context.log.info(df.info(verbose=True))
    
    raw = json.dumps(df.to_dict(orient="records"), indent=2)

    return Output(value=raw, metadata={
        "num_rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown())
    })
    
@asset
def tramitacoes_bronze(context: AssetExecutionContext, proposicoes_bronze: str) -> Output:
    """
    Extrai e processa dados de tramitações a partir das proposições.

    Args:
        context (AssetExecutionContext): O contexto da execução do asset.
        proposicoes_bronze (str): Dados de proposições processados em formato JSON.

    Returns:
        Output: Dados de tramitações em formato JSON.
    """
    proposicoes_json = json.loads(proposicoes_bronze)
    
    data = [{
        'createdAt': tramitacao.get('data', ''),
        'description': tramitacao.get('historico', ''),
        'local': tramitacao.get('local', ''),
        'propositionId': item.get('id', '')
    } for item in proposicoes_json if 'listaHistoricoTramitacoes' in item
        for tramitacao in item['listaHistoricoTramitacoes']]

    raw = json.dumps(data, indent=2)
    return Output(value=raw, metadata={
        "num_rows": len(data),
    })

@asset(io_manager_key="io_manager")
def tramitacoes_digest(context: AssetExecutionContext, tramitacoes_bronze: str) -> Output:
    """
    Processa os dados brutos de tramitações e retorna em formato Parquet.

    Args:
        context (AssetExecutionContext): O contexto da execução do asset.
        tramitacoes_bronze (str): Dados brutos de tramitações em formato JSON.

    Returns:
        Output: Dados de tramitações processados em formato Parquet.
    """
    data = json.loads(tramitacoes_bronze)
    columns = {
        "data": "createdAt",
        "historico": "description",
        "local": "local"
    }
    df = pd.DataFrame(data).rename(columns=columns)
    
    context.log.info(df.info(verbose=True))

    return Output(value=df.to_parquet(), metadata={
        "num_rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown())
    })
    
@asset(io_manager_key="silver_io_manager")
def tramitacoes_silver(context: AssetExecutionContext, tramitacoes_digest: bytes) -> Output:
    """
    Normaliza e ajusta os dados de tramitações em formato Parquet.

    Args:
        context (AssetExecutionContext): O contexto da execução do asset.
        tramitacoes_digest (bytes): Dados de tramitações em formato Parquet.

    Returns:
        Output: Dados de tramitações normalizados e ajustados.
    """
    pq_file = io.BytesIO(tramitacoes_digest)
    df = pd.read_parquet(pq_file)
    df['createdAt'] = pd.to_datetime(df['createdAt']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df = df_normalizer(df)
    
    return Output(value=df, metadata={
        "num_rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown())
    })

@asset(io_manager_key="io_manager")
def proposicoes_digest(context: AssetExecutionContext, proposicoes_raw: str) -> Output:
    """
    Processa os dados brutos de proposições e retorna em formato Parquet.

    Args:
        context (AssetExecutionContext): O contexto da execução do asset.
        proposicoes_raw (str): Dados brutos de proposições em formato JSON.

    Returns:
        Output: Dados de proposições processados em formato Parquet.
    """
    data = json.loads(proposicoes_raw)
    lista_itens = [item['resultado']['listaItem'] for item in data if 'resultado' in item and 'listaItem' in item['resultado']]

    columns = {
        "numero": "number",
        "autor": "author",
        "ano": "year",
        "dataPublicacao": "presentationDate",
        "ementa": "ementa",
        "regime": "regime",
        "situacao": "situation",
        "siglaTipoProjeto": "propositionType",
    }

    lista_itens_filtrada = [{k: v for k, v in item.items() if k in columns} for items in lista_itens for item in items]
    df = pd.DataFrame(lista_itens_filtrada).rename(columns=columns)
    df['city'] = 'Belo Horizonte'
    df['state'] = 'Minas Gerais'
    context.log.info(df.info(verbose=True))

    return Output(value=df.to_parquet(), metadata={
        "num_rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown())
    })

@asset(io_manager_key="silver_io_manager")
def proposicoes_silver(context: AssetExecutionContext, proposicoes_digest: bytes) -> Output:
    """
    Normaliza e ajusta os dados de proposições em formato Parquet.

    Args:
        context (AssetExecutionContext): O contexto da execução do asset.
        proposicoes_digest (bytes): Dados de proposições em formato Parquet.

    Returns:
        Output: Dados de proposições normalizados e ajustados.
    """
    pq_file = io.BytesIO(proposicoes_digest)
    df = pd.read_parquet(pq_file)

    df['presentationDate'] = pd.to_datetime(df['presentationDate']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df['number'] = df['number'].astype(str)
    df['year'] = df['year'].astype(int)
    df['ementa'] = df['ementa'].fillna(value="")

    context.log.info(df.info(verbose=True))

    df = df_normalizer(df)
    
    return Output(value=df, metadata={
        "num_rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown())
    })


@asset(checks=[AssetCheckResult(
    name="check_proposicoes_raw",
    description="Verifica se o número de itens em proposicoes_raw é maior que 0.",
    check=lambda context, value: len(json.loads(value)) > 0
)])
def check_proposicoes_raw(context: AssetExecutionContext, proposicoes_raw: str) -> AssetCheckResult:
    """
    Verifica a integridade dos dados brutos de proposições.
    """
    data = json.loads(proposicoes_raw)
    if len(data) > 0:
        return AssetCheckResult.passed()
    return AssetCheckResult.failed(message="proposicoes_raw não contém dados.")

@asset(checks=[AssetCheckResult(
    name="check_proposicoes_bronze",
    description="Verifica se o DataFrame de proposicoes_bronze possui linhas e não contém nulos essenciais após o processamento.",
    check=lambda context, value: check_dataframe_integrity(pd.read_json(value))
)])
def check_proposicoes_bronze(context: AssetExecutionContext, proposicoes_bronze: str) -> AssetCheckResult:
    """
    Verifica a integridade dos dados processados de proposições.
    """
    df = pd.read_json(proposicoes_bronze)
    if check_dataframe_integrity(df):
        return AssetCheckResult.passed()
    return AssetCheckResult.failed(message="proposicoes_bronze contém dados inválidos ou nulos.")

@asset(checks=[AssetCheckResult(
    name="check_tramitacoes_bronze",
    description="Verifica se o DataFrame de tramitacoes_bronze possui linhas e não contém nulos essenciais após o processamento.",
    check=lambda context, value: check_dataframe_integrity(pd.read_json(value))
)])
def check_tramitacoes_bronze(context: AssetExecutionContext, tramitacoes_bronze: str) -> AssetCheckResult:
    """
    Verifica a integridade dos dados de tramitações processados.
    """
    df = pd.read_json(tramitacoes_bronze)
    if check_dataframe_integrity(df):
        return AssetCheckResult.passed()
    return AssetCheckResult.failed(message="tramitacoes_bronze contém dados inválidos ou nulos.")

@asset(checks=[AssetCheckResult(
    name="check_proposicoes_digest",
    description="Verifica se o DataFrame de proposicoes_digest possui linhas e não contém nulos essenciais após o processamento.",
    check=lambda context, value: check_dataframe_integrity(pd.read_parquet(io.BytesIO(value)))
)])
def check_proposicoes_digest(context: AssetExecutionContext, proposicoes_digest: bytes) -> AssetCheckResult:
    """
    Verifica a integridade dos dados de proposições processados.
    """
    df = pd.read_parquet(io.BytesIO(proposicoes_digest))
    if check_dataframe_integrity(df):
        return AssetCheckResult.passed()
    return AssetCheckResult.failed(message="proposicoes_digest contém dados inválidos ou nulos.")

@asset(checks=[AssetCheckResult(
    name="check_tramitacoes_digest",
    description="Verifica se o DataFrame de tramitacoes_digest possui linhas e não contém nulos essenciais após o processamento.",
    check=lambda context, value: check_dataframe_integrity(pd.read_parquet(io.BytesIO(value)))
)])
def check_tramitacoes_digest(context: AssetExecutionContext, tramitacoes_digest: bytes) -> AssetCheckResult:
    """
    Verifica a integridade dos dados de tramitações processados.
    """
    df = pd.read_parquet(io.BytesIO(tramitacoes_digest))
    if check_dataframe_integrity(df):
        return AssetCheckResult.passed()
    return AssetCheckResult.failed(message="tramitacoes_digest contém dados inválidos ou nulos.")


def check_dataframe_integrity(df: pd.DataFrame) -> bool:
    """
    Verifica a integridade do DataFrame, garantindo que não contenha valores nulos
    nas colunas essenciais e que tenha pelo menos uma linha de dados.

    Args:
        df (pd.DataFrame): O DataFrame a ser verificado.

    Returns:
        bool: Retorna True se o DataFrame for considerado íntegro, caso contrário, False.
    """
    if df.empty:
        return False
    
    essential_columns = ['id', 'number', 'author', 'year', 'presentationDate', 'ementa', 'regime', 'situation', 'propositionType']
    missing_columns = [col for col in essential_columns if col not in df.columns]
    if missing_columns:
        return False
    
    for col in essential_columns:
        if df[col].isnull().any():
            return False
    
    return True