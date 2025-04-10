import polars as pl
import time
from emater_data_science.data.data_interface import DataInterface

def fGetSelicMensal(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(
            [
                pl.col("data").dt.year().alias("ano"),
                pl.col("data").dt.month().alias("mes"),
            ]
        )
        .group_by(["ano", "mes"])
        .agg(pl.col("valor").mean().alias("selic_mensal"))
        .sort(["ano", "mes"])
    )


def fCorrelacaoSelicPorProduto(selic: pl.DataFrame, safra: pl.DataFrame) -> dict[str, float]:
    # Normaliza tipos
    selic = selic.with_columns([pl.col("ano").cast(pl.Int32), pl.col("mes").cast(pl.Int32)])
    safra = safra.with_columns(
        [pl.col("nrAno").cast(pl.Int32).alias("ano"), pl.col("nrMes").cast(pl.Int32).alias("mes"), pl.col("produto")]
    )
    resultados: dict[str, float] = {}

    for produto in safra.select("produto").unique().to_series():
        dfProduto = (
            safra.filter(pl.col("produto") == produto)
            .group_by(["ano", "mes"])
            .agg(pl.col("nrProdutividade").mean().alias("produtividade_media"))
        )

        if dfProduto.is_empty():
            continue
        dfJoin = selic.join(dfProduto, on=["ano", "mes"], how="inner")
        if dfJoin.height < 3:  # pouco dado = correlação sem sentido
            continue

        correl = dfJoin.select(pl.corr("selic_mensal", "produtividade_media")).item()
        resultados[produto] = correl

    return resultados


def fAnalisarCorrelacoesSelic() -> dict[str, float]:
    resultado: dict[str, float] = {}

    def _onSelic(selicDf: pl.DataFrame):
        selicMensal = fGetSelicMensal(selicDf)

        def _onSafra(safraDf: pl.DataFrame):
            nonlocal resultado
            resultado = fCorrelacaoSelicPorProduto(selicMensal, safraDf)

        DataInterface().fFetchTable("dados_safra_emater", callback=_onSafra)

    DataInterface().fFetchTable("taxa_selic", callback=_onSelic)

    while not resultado:
        time.sleep(1)

    return resultado


if __name__ == "__main__":
    fAnalisarCorrelacoesSelic()
    DataInterface().fShutdown()
