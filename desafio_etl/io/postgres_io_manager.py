from dagster import (
    IOManager,
    io_manager,
    InitResourceContext,
    InputContext,
    OutputContext,
    StringSource
)
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()

Base = declarative_base()

class Proposicao(Base):
    __tablename__ = 'proposicao'
    id = Column(Integer, primary_key=True, autoincrement=True)
    author = Column(String)
    presentationDate = Column(DateTime)
    ementa = Column(String)
    regime = Column(String)
    situation = Column(String)
    propositionType = Column(String)
    number = Column(String)
    year = Column(Integer)
    city = Column(String, default="Belo Horizonte")
    state = Column(String, default="Minas Gerais")

class Tramitacao(Base):
    __tablename__ = 'tramitacao'
    id = Column(Integer, primary_key=True, autoincrement=True)
    createdAt = Column(DateTime)
    description = Column(String)
    local = Column(String)
    propositionId = Column(Integer, ForeignKey('proposicao.id'))

class PostgresPandasIOManager(IOManager):
    def __init__(self, user: str, password: str, host: str, database: str, port: str) -> None:
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}')

    def handle_output(self, context: OutputContext, obj):
        if obj is None:
            return

        table_name = context.asset_key.to_python_identifier()

        # Ensuring table creation
        if table_name == 'proposicao':
            Base.metadata.tables['proposicao'].create(self.engine, checkfirst=True)
        elif table_name == 'tramitacao':
            Base.metadata.tables['tramitacao'].create(self.engine, checkfirst=True)

        obj.to_sql(table_name, self.engine, if_exists='replace', index=False)
        context.add_output_metadata({"db": self.database, "table_name": table_name})

    def load_input(self, context: InputContext):
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
    return PostgresPandasIOManager(
        user=os.getenv('SILVER_DB_USER'),
        password=os.getenv('SILVER_DB_PASSWORD'),
        host=os.getenv('SILVER_DB_HOST'),
        database=os.getenv('SILVER_DB_NAME'),
        port=os.getenv('SILVER_DB_PORT'),
    )