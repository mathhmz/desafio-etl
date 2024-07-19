from typing import Any
from dagster import (AssetExecutionContext,asset, Output, MetadataValue)
from .core.services.proposicoes_service import ProposicoesService
from .utils.dataframe_normalize import df_normalizer
import json
import pandas as pd

@asset(io_manager_key="io_manager")
def proposicoes_raw(context: AssetExecutionContext):
    
    service_request = ProposicoesService()
    data = service_request()
    raw = json.dumps(data, indent=2)
    

    return Output(value = raw, metadata={
        "num_rows": len(raw)
    })

@asset
def tramitacoes_raw(context: AssetExecutionContext, proposicoes_raw):
    
    data = json.loads(proposicoes_raw)
    
    list_itens = []
    
    for item in data:
        if 'resultado' in item and 'listaItem' in item['resultado']:
            list_itens.extend(item['resultado']['listaItem'])  
    
    data = []
    
    for item in list_itens:
        if 'listaHistoricoTramitacoes' in item:
            for tramitacao in item['listaHistoricoTramitacoes']:
                data.append({
                    'numero': item.get('numero', ''),
                    'createdAt': tramitacao.get('data', ''),
                    'description': tramitacao.get('historico', ''),
                    'local': tramitacao.get('local', '')
                })
                
    raw = json.dumps(data, indent=2)
    return Output(value = raw, metadata={
        "num_rows": len(raw),
    })
    
    
@asset
def tramitacoes_digest(context: AssetExecutionContext, tramitacoes_raw):
    
    data = json.loads(tramitacoes_raw)
    columns = {"numero":"number",
          "data":"CreatedAt",
          "historico":"description",
          "local":"local"
}
    df = pd.DataFrame(data).rename(columns=columns)
    context.log.info(df.info(verbose=True))

    return Output(value= df, metadata={
        "num_rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown())
    })
    
@asset
def tramitacoes_silver(context: AssetExecutionContext, tramitacoes_digest):
    tramitacoes_digest['number'] = tramitacoes_digest['number'].astype(str)
    tramitacoes_digest['createdAt'] = pd.to_datetime(tramitacoes_digest['createdAt']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df = df_normalizer(tramitacoes_digest)
    return Output(value= df.to_parquet(), metadata={
        "num_rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown())
    })

    
    
@asset(io_manager_key="io_manager")
def proposicoes_digest(context: AssetExecutionContext, proposicoes_raw):
    data = json.loads(proposicoes_raw)

    lista_itens = []
    for item in data:
        if 'resultado' in item and 'listaItem' in item['resultado']:
            lista_itens.extend(item['resultado']['listaItem'])      
    
    columns = {"numero":"number",
          "autor":"author",
          "ano":"year",
          "dataPublicacao":"presentationDate",
          "ementa":"ementa",
          "regime":"regime",
          "situacao":"situation",
          "siglaTipoProjeto":"propositionType",
}
    

    lista_itens_filtrada = [{k: v for k, v in item.items() if k in columns} for item in lista_itens]

    df = pd.DataFrame(lista_itens_filtrada).rename(columns=columns)
    df['city'] = 'Belo Horizonte'
    df['state'] = 'Minas Gerais'
    context.log.info(df.info(verbose=True))

    return Output(value= df, metadata={
        "num_rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown())
    })
    
@asset
def proposicoes_silver(context: AssetExecutionContext, proposicoes_digest):
    
    proposicoes_digest['presentationDate'] = pd.to_datetime(proposicoes_digest['presentationDate']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    proposicoes_digest['number'] = proposicoes_digest['number'].astype(str)
    
    proposicoes_digest['year'] = proposicoes_digest['year'].astype(int)
    
    #Preenchendo os valores nulos, pois a ementa não é presente em todas as proposições.
    #Como em outros campos desses dados, preencho os valores nulos com uma string vazia;
    proposicoes_digest['ementa'] = proposicoes_digest['ementa'].fillna(value="")
    
    context.log.info(proposicoes_digest.info(verbose=True))
    
    df = df_normalizer(proposicoes_digest)
    
    return Output(value= df.to_parquet(), metadata={
        "num_rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown())
    })
    
    

