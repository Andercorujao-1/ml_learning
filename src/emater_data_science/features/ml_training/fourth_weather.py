import polars as pl
import time
from emater_data_science.data.data_interface import DataInterface

def fAgruparMeteorologiaMensal(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns([
        pl.col("data").dt.year().alias("ano"),
        pl.col("data").dt.month().alias("mes")
    ])

    return (
        df.group_by(["ano", "mes"])
        .agg([
            pl.col("precipitacao").sum().alias("precipitacao_total_dias"),
            pl.col("precipitacao").mean().alias("precipitacao_media_uf"),
            pl.col("pressao").mean().alias("pressao_media"),
            pl.col("radiacao").mean().alias("radiacao_media"),
            pl.col("temp_bulbo_seco").mean().alias("temperatura_media"),
            pl.col("umidade").mean().alias("umidade_media"),
            pl.col("vento_vel").mean().alias("vento_vel_media")
        ])
        .sort(["ano", "mes"])
    )

def fCorrelacaoClimaPorProduto(meteorologia: pl.DataFrame, safra: pl.DataFrame) -> dict[str, dict[str, float ]]:
    meteorologia = meteorologia.with_columns([
        pl.col("ano").cast(pl.Int32),
        pl.col("mes").cast(pl.Int32)
    ])
    safra = safra.with_columns([
        pl.col("nrAno").cast(pl.Int32).alias("ano"),
        pl.col("nrMes").cast(pl.Int32).alias("mes"),
        pl.col("produto")
    ])

    resultados: dict[str, dict[str, float ]] = {}
    colunas_meteorologicas = [
        "precipitacao_total_dias",
        "precipitacao_media_uf",
        "pressao_media",
        "radiacao_media",
        "temperatura_media",
        "umidade_media",
        "vento_vel_media"
    ]

    for produto in safra.select("produto").unique().to_series():
        dfProduto = (
            safra.filter(pl.col("produto") == produto)
            .group_by(["ano", "mes"])
            .agg(pl.col("nrProdutividade").mean().alias("produtividade_media"))
        )

        if dfProduto.is_empty():
            continue

        dfJoin = meteorologia.join(dfProduto, on=["ano", "mes"], how="inner")
        if dfJoin.height < 3:
            continue

        correlacoes: dict[str, float ] = {}
        for coluna in colunas_meteorologicas:
            valor = dfJoin.select(pl.corr(coluna, "produtividade_media")).item()
            correlacoes[coluna] =  valor

        resultados[produto] = correlacoes

    return resultados

def fAnalisarMeteorologia() -> dict[str, dict[str, float ]]:
    dadosMet = None
    dadosSafra = None

    def fSetMet(d: pl.DataFrame):
        nonlocal dadosMet
        dadosMet = d

    def fSetSafra(d: pl.DataFrame):
        nonlocal dadosSafra
        dadosSafra = d

    DataInterface().fFetchTable("estacao_inmet_meteorologico_diario", callback=fSetMet)
    DataInterface().fFetchTable("dados_safra_emater", callback=fSetSafra)

    while dadosMet is None or dadosSafra is None:
        time.sleep(1)

    metMensal = fAgruparMeteorologiaMensal(dadosMet)
    resultado = fCorrelacaoClimaPorProduto(metMensal, dadosSafra)

    return resultado

if __name__ == "__main__":
    resultado = fAnalisarMeteorologia()
    print("\n📊 Correlação CLIMA vs Produtividade por produto:")
    for produto, correlacoes in sorted(
        resultado.items(),
        key=lambda x: -max([
            abs(v) for v in x[1].values()
            if isinstance(v, float) and v == v
        ] or [0.0])
    ):
        print(f"{produto:40}")
        for k, v in correlacoes.items():
            if v is None or (isinstance(v, float) and v != v):
                print(f"   {k:26} → sem dados")
            else:
                print(f"   {k:26} → {v:.4f}")
    DataInterface().fShutdown()
