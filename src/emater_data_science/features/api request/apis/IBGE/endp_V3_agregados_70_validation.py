# endp_V3_agregados_70_validation.py
from pydantic import BaseModel
from typing import Optional, List, Dict
import logging


class ClassificacoesModel(BaseModel):
    id: str
    nome: Optional[str]
    categoria: Optional[Dict]

    @staticmethod
    def fromJson(data):
        try:
            return ClassificacoesModel(**data)
        except Exception as e:
            logging.error((f"Unexpected error ClassificacoesModel: {e}"))
            raise


class SeriesModel(BaseModel):
    localidade_id: str
    localidade_nome: Optional[str]
    nivel: Optional[Dict]
    serie: Dict

    @staticmethod
    def fromJson(data):
        try:
            return SeriesModel(**data)
        except Exception as e:
            logging.error((f"Unexpected error SeriesModel: {e}"))
            raise


class ResultadosModel(BaseModel):
    classificacoes: List[ClassificacoesModel]
    series: List[SeriesModel]

    @staticmethod
    def fromJson(data):
        try:
            return ResultadosModel(**data)
        except Exception as e:
            logging.error((f"Unexpected error ResultadosModel: {e}"))
            raise


class AbateDeAnimaisModel(BaseModel):
    id: str
    variavel: str
    unidade: str
    resultados: Optional[ResultadosModel]

    @staticmethod
    def fromJson(data):
        try:
            return AbateDeAnimaisModel(**data)
        except Exception as e:
            logging.error((f"Unexpected error AbateDeAnimaisModel: {e}"))
            raise
