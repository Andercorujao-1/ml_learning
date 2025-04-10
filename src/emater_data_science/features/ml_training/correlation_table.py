import polars as pl
from emater_data_science.data.data_interface import DataInterface
from emater_data_science.features.ml_training.first_selic import fAnalisarCorrelacoesSelic
from emater_data_science.features.ml_training.second_dollar import fAnalisarCorrelacoesDollar
from emater_data_science.features.ml_training.third_credito import fAnalisarCorrelacoesCredito
from emater_data_science.features.ml_training.fourth_weather import fAnalisarMeteorologia
from sqlalchemy import Column, String, Float
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    pass

class CorrelacoesProdutividade(Base):
    __tablename__ = "correlacoes_produtividade"

    produto = Column(String, primary_key=True)
    fonte = Column(String, primary_key=True)
    variavel = Column(String, primary_key=True)
    correlacao = Column(Float)

def fConverterResultadosParaTabela(nomeFonte: str, resultados: dict[str, float] | dict[str, dict[str, float ]]) -> pl.DataFrame:
    linhas = []
    for produto, valor in resultados.items():
        if isinstance(valor, dict):
            for variavel, correlacao in valor.items():
                linhas.append({
                    "produto": produto,
                    "fonte": nomeFonte,
                    "variavel": variavel,
                    "correlacao": correlacao
                })
        else:
            linhas.append({
                "produto": produto,
                "fonte": nomeFonte,
                "variavel": "valor",
                "correlacao": valor
            })
    return pl.DataFrame(linhas)


def fGerarTabelaCorrelacoes() :
    clima = fAnalisarMeteorologia()
    selic = fAnalisarCorrelacoesSelic()
    dollar = fAnalisarCorrelacoesDollar()
    credito = fAnalisarCorrelacoesCredito()

    tabelaClima = fConverterResultadosParaTabela("clima", clima)
    tabelaSelic = fConverterResultadosParaTabela("selic", selic)
    tabelaDollar = fConverterResultadosParaTabela("dollar", dollar)
    tabelaCredito = fConverterResultadosParaTabela("credito", credito)

    return pl.concat([tabelaClima, tabelaSelic, tabelaDollar, tabelaCredito], how="vertical")


if __name__ == "__main__":
    tabelaFinal = fGerarTabelaCorrelacoes()
    DataInterface().fStoreTable(CorrelacoesProdutividade, tabelaFinal)
    DataInterface().fShutdown()
