from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import  Column, ForeignKey, Integer, String, DateTime, text
from sqlalchemy.orm import relationship, sessionmaker


Base = declarative_base()

class ProposicaoSchema(Base):
    __tablename__ = 'proposicoes_silver'
    id = Column(Integer, primary_key=True)
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
    
    tramitacoes_silver = relationship("TramitacaoSchema", back_populates="proposicoes_silver")

class TramitacaoSchema(Base):
    __tablename__ = 'tramitacoes_silver'
    id = Column(Integer, primary_key=True, autoincrement=True)
    createdAt = Column(DateTime)
    description = Column(String)
    local = Column(String)
    propositionId = Column(Integer, ForeignKey('proposicoes_silver.id'))
    
    proposicoes_silver = relationship("ProposicaoSchema", back_populates="tramitacoes_silver")
    
    

def create_and_update_table(engine, table_name, df):
    
    Base.metadata.delete(engine, table_name)
    
    
    
    Base.metadata.tables['proposicoes_silver'].create(engine, checkfirst=True)
    Base.metadata.tables['tramitacoes_silver'].create(engine, checkfirst=True)
    

    df.to_sql(table_name, engine, if_exists='append', index=False)
