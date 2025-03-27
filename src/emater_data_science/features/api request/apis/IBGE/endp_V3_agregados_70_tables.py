# endp_V3_agregados_70_validation.py
from sqlalchemy import Column, ForeignKey, Integer, String, JSON
from sqlalchemy.orm import relationship, declarative_base

from .endp_V3_agregados_70_validation import (
    ClassificacoesModel,
    SeriesModel,
    ResultadosModel,
    AbateDeAnimaisModel,
)

Base = declarative_base()


class Classificacoes(Base):
    __tablename__ = "abateDeAnimais_resultados_classificacoes"
    id = Column(String, primary_key=True)
    nome = Column(String)
    categoria = Column(JSON)  # Storing category as JSON for flexibility
    resultados_id = Column(Integer, ForeignKey("resultados.id"))

    @staticmethod
    def fromJson(data):
        validated_data = ClassificacoesModel.fromJson(data)
        return Classificacoes(**validated_data)


class Series(Base):
    __tablename__ = "abateDeAnimais_resultados_series"
    id = Column(Integer, primary_key=True, autoincrement=True)
    localidade_id = Column(String)
    localidade_nome = Column(String)
    nivel = Column(JSON)
    serie = Column(JSON)  # Historical data as JSON
    resultados_id = Column(Integer, ForeignKey("resultados.id"))

    @staticmethod
    def fromJson(data):
        validated_data = SeriesModel.fromJson(data)
        return Series(**validated_data)


class Resultados(Base):
    __tablename__ = "abateDeAnimais_resultados"
    id = Column(Integer, primary_key=True, autoincrement=True)
    classificacoes = relationship("Classificacoes", backref="resultados")
    series = relationship("Series", backref="resultados")

    @staticmethod
    def fromJson(data):
        validated_data = ResultadosModel.fromJson(data)
        return Resultados(**validated_data)


class AbateDeAnimais(Base):
    __tablename__ = "abateDeAnimais"
    id = Column(String, primary_key=True)
    variavel = Column(String)
    unidade = Column(String)
    resultados_id = Column(Integer, ForeignKey("resultados.id"))
    resultados = relationship("Resultados", backref="AbateDeAnimais")

    @staticmethod
    def fromJson(data) -> list["AbateDeAnimais"]:
        listOfRowObjects = []
        for rowData in data:
            validatedData = AbateDeAnimaisModel.fromJson(rowData)
            listOfRowObjects.append(AbateDeAnimais(**validatedData))
        return listOfRowObjects
