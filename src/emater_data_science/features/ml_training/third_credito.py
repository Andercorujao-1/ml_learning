import polars as pl
from emater_data_science.data.data_interface import DataInterface
import time

def fGetCreditoRuralMensal(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns([
            pl.col("data").dt.year().alias("ano"),
            pl.col("data").dt.month().alias("mes"),
        ])
        .group_by(["ano", "mes"])
        .agg([
            pl.col("vl_custeio").sum().alias("custeio_mensal"),
            pl.col("vl_investimento").sum().alias("investimento_mensal"),
            pl.col("vl_comercializacao").sum().alias("comercializacao_mensal"),
        ])
        .sort(["ano", "mes"])
    )

def fCorrelacaoCreditoPorProduto(credito: pl.DataFrame, safra: pl.DataFrame) -> dict[str, dict[str, float]]:
    credito = credito.with_columns([
        pl.col("ano").cast(pl.Int32),
        pl.col("mes").cast(pl.Int32),
    ])
    safra = safra.with_columns([
        pl.col("nrAno").cast(pl.Int32).alias("ano"),
        pl.col("nrMes").cast(pl.Int32).alias("mes"),
        pl.col("produto"),
    ])

    resultados: dict[str, dict[str, float]] = {}

    for produto in safra.select("produto").unique().to_series():
        dfProduto = (
            safra.filter(pl.col("produto") == produto)
            .group_by(["ano", "mes"])
            .agg(pl.col("nrProdutividade").mean().alias("produtividade_media"))
        )

        if dfProduto.is_empty():
            continue

        dfJoin = credito.join(dfProduto, on=["ano", "mes"], how="inner")

        if dfJoin.height < 3:
            continue

        resultados[produto] = {
            "custeio": dfJoin.select(pl.corr("custeio_mensal", "produtividade_media")).item(),
            "investimento": dfJoin.select(pl.corr("investimento_mensal", "produtividade_media")).item(),
            "comercializacao": dfJoin.select(pl.corr("comercializacao_mensal", "produtividade_media")).item(),
        }

    return resultados

def fAnalisarCorrelacoesCredito() -> dict[str, dict[str, float]]:
    dadosSafra = None
    dadosCredito = None

    def fSetSafra(safraDf: pl.DataFrame):
        nonlocal dadosSafra
        dadosSafra = safraDf

    def fSetCredito(creditoDf: pl.DataFrame):
        nonlocal dadosCredito
        dadosCredito = creditoDf

    DataInterface().fFetchTable("dados_safra_emater", callback=fSetSafra)
    DataInterface().fFetchTable("credito_rural", callback=fSetCredito)

    while dadosSafra is None or dadosCredito is None:
        time.sleep(1)

    creditoMensal = fGetCreditoRuralMensal(dadosCredito)
    resultados = fCorrelacaoCreditoPorProduto(creditoMensal, dadosSafra)

    return resultados

if __name__ == "__main__":
    fAnalisarCorrelacoesCredito()
    DataInterface().fShutdown()
