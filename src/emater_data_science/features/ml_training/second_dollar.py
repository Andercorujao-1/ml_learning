import polars as pl
from emater_data_science.data.data_interface import DataInterface
import time

def fGetDolarMensal(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns([
            pl.col("data").dt.year().alias("ano"),
            pl.col("data").dt.month().alias("mes"),
        ])
        .group_by(["ano", "mes"])
        .agg(
            [
                pl.col("cotacao_compra").mean().alias("dolar_compra_mensal"),
                pl.col("cotacao_venda").mean().alias("dolar_venda_mensal"),
            ]
        )
        .sort(["ano", "mes"])
    )

def fCorrelacaoDolarPorProduto(dolar: pl.DataFrame, safra: pl.DataFrame) -> dict[str, float]:
    # Normaliza tipos
    dolar = dolar.with_columns([
        pl.col("ano").cast(pl.Int32),
        pl.col("mes").cast(pl.Int32),
    ])
    safra = safra.with_columns([
        pl.col("nrAno").cast(pl.Int32).alias("ano"),
        pl.col("nrMes").cast(pl.Int32).alias("mes"),
        pl.col("produto"),
    ])

    resultados: dict[str, float] = {}

    for produto in safra.select("produto").unique().to_series():
        dfProduto = (
            safra.filter(pl.col("produto") == produto)
            .group_by(["ano", "mes"])
            .agg(pl.col("nrProdutividade").mean().alias("produtividade_media"))
        )

        if dfProduto.is_empty():
            continue

        dfJoin = dolar.join(dfProduto, on=["ano", "mes"], how="inner")

        if dfJoin.height < 3:
            continue

        correl = dfJoin.select(pl.corr("dolar_venda_mensal", "produtividade_media")).item()
        resultados[produto] = correl

    return resultados

def fAnalisarCorrelacoesDollar():
    resultado: dict[str, float] = {}
    def _onDolar(dolarDf: pl.DataFrame):
        dolarMensal = fGetDolarMensal(dolarDf)

        def _onSafra(safraDf: pl.DataFrame):
            nonlocal resultado
            resultado = fCorrelacaoDolarPorProduto(dolarMensal, safraDf)

        DataInterface().fFetchTable("dados_safra_emater", callback=_onSafra)

    DataInterface().fFetchTable("cotacao_dolar", callback=_onDolar)

    while not resultado:
        time.sleep(1)

    return resultado

if __name__ == "__main__":
    fAnalisarCorrelacoesDollar()
    DataInterface().fShutdown()