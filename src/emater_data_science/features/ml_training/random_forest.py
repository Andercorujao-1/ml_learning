import polars as pl
import time
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
from emater_data_science.data.data_interface import DataInterface

def fCarregarTabelas():
    safra = selic = credito = dolar = clima = None

    def fSetSafra(df):
        nonlocal safra
        print("Carregando dados de safra...")
        safra = df.with_columns([
            pl.col("nrAno").cast(pl.Int32),
            pl.col("nrMes").cast(pl.Int32)
        ])

    def fSetSelic(df):
        nonlocal selic
        print("Carregando dados de taxa Selic...")
        selic = df.with_columns([
            pl.col("data").dt.year().cast(pl.Int32).alias("nrAno"),
            pl.col("data").dt.month().cast(pl.Int32).alias("nrMes")
        ])

    def fSetCredito(df):
        nonlocal credito
        print("Carregando dados de crédito rural...")
        credito = df.with_columns([
            pl.col("data").dt.year().cast(pl.Int32).alias("nrAno"),
            pl.col("data").dt.month().cast(pl.Int32).alias("nrMes")
        ])

    def fSetDolar(df):
        nonlocal dolar
        print("Carregando dados de cotação do dólar...")
        dolar = df.with_columns([
            pl.col("data").dt.year().cast(pl.Int32).alias("nrAno"),
            pl.col("data").dt.month().cast(pl.Int32).alias("nrMes")
        ])

    def fSetClima(df):
        print("Carregando dados de clima...")
        nonlocal clima
        clima = df

    dataInterface = DataInterface()
    dataInterface.fFetchTable("dados_safra_emater", callback=fSetSafra)
    dataInterface.fFetchTable("taxa_selic", callback=fSetSelic)
    dataInterface.fFetchTable("credito_rural", callback=fSetCredito)
    dataInterface.fFetchTable("cotacao_dolar", callback=fSetDolar)
    dataInterface.fFetchTable("estacao_inmet_meteorologico_diario", callback=fSetClima)

    while safra is None or selic is None or credito is None or dolar is None or clima is None:
        time.sleep(0.2)

    return safra, selic, credito, dolar, clima

def fMontarBase(safra, selic, credito, dolar, clima, produto):
    produtoDf = safra.filter(pl.col("produto") == produto)

    climaMensal = clima.group_by([
        pl.col("data").dt.year().cast(pl.Int32).alias("nrAno"),
        pl.col("data").dt.month().cast(pl.Int32).alias("nrMes")
    ]).agg([
        pl.col("precipitacao").sum().alias("precipitacao"),
        pl.col("pressao").mean().alias("pressao"),
        pl.col("radiacao").mean().alias("radiacao"),
        pl.col("temp_bulbo_seco").mean().alias("temp_bulbo_seco"),
        pl.col("umidade").mean().alias("umidade"),
        pl.col("vento_vel").mean().alias("vento_vel")
    ])

    dolarMensal = dolar.group_by(["nrAno", "nrMes"]).agg([
        pl.col("cotacao_venda").mean().alias("dolar")
    ])
    selicMensal = selic.group_by(["nrAno", "nrMes"]).agg([
        pl.col("valor").mean().alias("selic")
    ])
    creditoMensal = credito.group_by(["nrAno", "nrMes"]).agg([
        pl.col("vl_comercializacao").sum().alias("comercializacao"),
        pl.col("vl_custeio").sum().alias("custeio"),
        pl.col("vl_investimento").sum().alias("investimento"),
        (pl.col("vl_comercializacao") + pl.col("vl_custeio") + pl.col("vl_investimento")).sum().alias("valorTotal")
    ])

    base = produtoDf.join(climaMensal, on=["nrAno", "nrMes"], how="inner")
    base = base.join(dolarMensal, on=["nrAno", "nrMes"], how="inner")
    base = base.join(selicMensal, on=["nrAno", "nrMes"], how="inner")
    base = base.join(creditoMensal, on=["nrAno", "nrMes"], how="inner")

    return base

def fPrepararFeaturesELabel(df: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    colunasAlvo = "nrProdutividade"
    colunasNumericas = [
        "comercializacao", "custeio", "investimento", "valorTotal",
        "precipitacao", "pressao", "radiacao",
        "temp_bulbo_seco", "umidade", "vento_vel",
        "selic", "dolar"
    ]
    dfNumerico = df.select(colunasNumericas + [colunasAlvo]).drop_nulls()

    descr = dfNumerico.select(colunasAlvo).describe()
    q1 = descr.filter(pl.col("statistic") == "25%").select(colunasAlvo).item()
    q3 = descr.filter(pl.col("statistic") == "75%").select(colunasAlvo).item()
    iqr = q3 - q1
    limiteInferior = q1 - 1.5 * iqr
    limiteSuperior = q3 + 1.5 * iqr

    dfFiltrado = dfNumerico.filter(
        (pl.col(colunasAlvo) >= limiteInferior) & (pl.col(colunasAlvo) <= limiteSuperior)
    )

    print("\U0001F4CA Descrição da variável alvo (produtividade) após remover outliers:")
    print(dfFiltrado.select(colunasAlvo).describe())
    print("\U0001F522 Número de registros para treino:", dfFiltrado.shape[0])

    return dfFiltrado.select(colunasNumericas).to_numpy(), dfFiltrado[colunasAlvo].to_numpy()

def fTreinarModeloRandomForest(X, y):
    scaler = StandardScaler()
    X = scaler.fit_transform(X)
    X_treino, X_teste, y_treino, y_teste = train_test_split(X, y, test_size=0.2, random_state=42)
    modelo = RandomForestRegressor(n_estimators=100, random_state=42)
    modelo.fit(X_treino, y_treino)
    previsoes = modelo.predict(X_teste)
    print("MSE:", mean_squared_error(y_teste, previsoes))
    print("R²:", r2_score(y_teste, previsoes))
    return modelo

def fExecutarPrevisaoProduto(produto="Alface"):
    safra, selic, credito, dolar, clima = fCarregarTabelas()
    print(f"\n📦 Produto selecionado: {produto}\n")
    base = fMontarBase(safra, selic, credito, dolar, clima, produto)
    X, y = fPrepararFeaturesELabel(base)
    modelo = fTreinarModeloRandomForest(X, y)
    return modelo

if __name__ == "__main__":
    fExecutarPrevisaoProduto()
    DataInterface().fShutdown()