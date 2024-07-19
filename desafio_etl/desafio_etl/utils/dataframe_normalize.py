#Criando uma função que remove os espaços em branco e busca eliminar qualquer outro caracter especial ou regex que possa causar conflito;
import re
def df_normalizer(df):
    for col in df.select_dtypes(include=['object']).columns:
        # Remover espaços em branco do início e fim
        df[col] = df[col].str.strip()
        # Remover múltiplos espaços em branco
        df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
        # Remover qualquer outra expressão regular indesejada
        df[col] = df[col].str.replace(r'[^\w\s,.-]', '', regex=True)
    return df