from dagster import (
    IOManager,
    io_manager,
    InitResourceContext,
    InputContext,
    OutputContext,
    StringSource
)
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()

Base = declarative_base()

class Proposicao(Base):
    __tablename__ = 'proposicoes_silver'
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
    __tablename__ = 'tramitacoes_silver'
    id = Column(Integer, primary_key=True, autoincrement=True)
    createdAt = Column(DateTime)
    description = Column(String)
    local = Column(String)

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

        if table_name == 'proposicoes_silver':
            Base.metadata.tables['proposicoes_silver'].create(self.engine, checkfirst=True)
        elif table_name == 'tramitacoes_silver':
            Base.metadata.tables['tramitacoes_silver'].create(self.engine, checkfirst=True)

        obj.to_sql(table_name, self.engine, if_exists='replace', index=False)
        with self.engine.connect() as conn:
            if table_name == 'proposicoes_silver':
                conn.execute(text("ALTER TABLE proposicoes_silver  ADD COLUMN id BIGSERIAL PRIMARY KEY;"))
                conn.commit()
            elif table_name == 'tramitacoes_silver':
                conn.execute(text("ALTER TABLE tramitacoes_silver ADD COLUMN propositionID bigint;"))
                conn.commit()
                conn.execute(text("UPDATE tramitacoes_silver ts SET propositionID = ps.id FROM proposicoes_silver ps WHERE ts.number = ps.number;"))
                conn.commit()
                conn.execute(text("ALTER TABLE tramitacoes_silver ADD CONSTRAINT fk_propositionID FOREIGN KEY (propositionID) REFERENCES proposicoes_silver(id)"))
                conn.commit()
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