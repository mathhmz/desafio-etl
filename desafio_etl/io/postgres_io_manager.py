from dagster import IOManager, io_manager, InitResourceContext, InputContext, OutputContext, StringSource
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from ..database.base import create_and_update_table



# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()



class PostgresPandasIOManager(IOManager):
    """
    Classe para gerenciar I/O no Dagster usando PostgreSQL e Pandas.

    Métodos:
        __init__(user, password, host, database, port): Inicializa a conexão com o banco de dados.
        handle_output(context, obj): Salva um DataFrame em uma tabela PostgreSQL.
        load_input(context): Carrega dados de uma tabela PostgreSQL em um DataFrame.
    """
    def __init__(self, user: str, password: str, host: str, database: str, port: str) -> None:
        """
        Inicializa a conexão com o banco de dados.

        Args:
            user (str): Nome de usuário do banco de dados.
            password (str): Senha do banco de dados.
            host (str): Endereço do host do banco de dados.
            database (str): Nome do banco de dados.
            port (str): Porta de conexão do banco de dados.
        """
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.port = port
        self.engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}')

    def handle_output(self, context: OutputContext, obj):
        """
        Salva um DataFrame em uma tabela PostgreSQL.

        Args:
            context (OutputContext): Contexto de saída do Dagster.
            obj (DataFrame): Objeto DataFrame a ser salvo.
        """
        if obj is None:
            return

        table_name = context.asset_key.to_python_identifier()
        create_and_update_table(self.engine, table_name, obj)
        context.add_output_metadata({"db": self.database, "table_name": table_name})

    def load_input(self, context: InputContext):
        """
        Carrega dados de uma tabela PostgreSQL em um DataFrame.

        Args:
            context (InputContext): Contexto de entrada do Dagster.

        Returns:
            DataFrame: DataFrame contendo os dados carregados da tabela.
        """
        table_name = context.upstream_output.asset_key.to_python_identifier()
        df = pd.read_sql(f"SELECT * FROM public.{table_name}", self.engine)
        return df

@io_manager(
    config_schema={
        "user": StringSource,
        "password": StringSource,
        "host": StringSource,
        "database": StringSource,
        "port": StringSource,
    }
)
def postgres_pandas_io_manager(init_context: InitResourceContext) -> PostgresPandasIOManager:
    """
    Configuração do IO Manager no Dagster.

    Args:
        init_context (InitResourceContext): Contexto de inicialização do recurso no Dagster.

    Returns:
        PostgresPandasIOManager: Instância do gerenciador de I/O configurado.
    """
    return PostgresPandasIOManager(
        user=os.getenv('SILVER_DB_USER'),
        password=os.getenv('SILVER_DB_PASSWORD'),
        host=os.getenv('SILVER_DB_HOST'),
        database=os.getenv('SILVER_DB_NAME'),
        port=os.getenv('SILVER_DB_PORT'),
    )
