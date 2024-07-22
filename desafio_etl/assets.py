from typing import Any
from dagster import (AssetExecutionContext, asset, Output, MetadataValue)
from .core.services.proposicoes_service import ProposicoesService
from .utils.dataframe_normalize import df_normalizer
import json
import pandas as pd
import io

@asset(io_manager_key="io_manager")
def proposicoes_raw(context: AssetExecutionContext) -> Output:
    
    service_request = ProposicoesService()
    data = service_request()
    raw = json.dumps(data, indent=2)

    return Output(value=raw, metadata={
        "num_rows": len(data)
    })
    
    
@asset
def proposicoes_bronze(context: AssetExecutionContext, proposicoes_raw: str) -> Output:
    data = json.loads(proposicoes_raw)
    lista_itens = [item['resultado']['listaItem'] for item in data if 'resultado' in item and 'listaItem' in item['resultado']]

    columns = {
        "index" : "id",
        "numero": "number",
        "autor": "author",
        "ano": "year",
        "dataPublicacao": "presentationDate",
        "ementa": "ementa",
        "regime": "regime",
        "situacao": "situation",
        "siglaTipoProjeto": "propositionType",
        "listaHistoricoTramitacoes" : "listaHistoricoTramitacoes"
        
    }

    lista_itens_filtrada = [{k: v for k, v in item.items() if k in columns} for items in lista_itens for item in items]
    df = pd.DataFrame(lista_itens_filtrada)
    df = df.reset_index()
    df = df.rename(columns=columns)
    df['id'] = df['id'].astype(int) + 1

    context.log.info(df.info(verbose=True))
    
    raw = json.dumps(df.to_dict(orient="records"), indent=2)

    return Output(value= raw, metadata={
        "num_rows": len(raw),
        "preview": MetadataValue.md(df.head(10).to_markdown())
    })
    
@asset 
def tramitacoes_bronze(context: AssetExecutionContext, proposicoes_bronze: str) -> Output:
    proposicoes_json = json.loads(proposicoes_bronze)
    
    
    data = []
    for item in proposicoes_json:
            if 'listaHistoricoTramitacoes' in item:
                for tramitacao in item['listaHistoricoTramitacoes']:
                    data.append({
                        'createdAt': tramitacao.get('data', ''),
                        'description': tramitacao.get('historico', ''),
                        'local': tramitacao.get('local', ''),
                        'propositionId' : item.get('id', '')
                    })

    raw = json.dumps(data, indent=2)
    return Output(value=raw, metadata={
        "num_rows": len(data),
    })




@asset(io_manager_key="io_manager")
def tramitacoes_digest(context: AssetExecutionContext, tramitacoes_bronze: str) -> Output:
    """
    Processa os dados brutos de tramitações em um DataFrame e retorna em formato Parquet.

    Args:
        context (AssetExecutionContext): O contexto da execução do asset.
        tramitacoes_raw (str): Dados brutos de tramitações.

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
    Processa os dados brutos de proposições em um DataFrame e retorna em formato Parquet.

    Args:
        context (AssetExecutionContext): O contexto da execução do asset.
        proposicoes_raw (str): Dados brutos de proposições.

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
