import re
import pandas as pd

def df_normalizer(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=['object']).columns:
        # Remover espaços em branco do início e fim
        df[col] = df[col].str.strip()
        # Remover múltiplos espaços em branco
        df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
        # Remover caracteres especiais, mantendo letras, números e espaços
        df[col] = df[col].str.replace(r'[^\w\s,.-]', '', regex=True)
        # Opcional: remover espaços extras dentro de palavras
        df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
    return df