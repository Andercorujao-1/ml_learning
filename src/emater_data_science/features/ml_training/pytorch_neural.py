from datetime import date
import polars as pl
import time
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
from emater_data_science.data.data_interface import DataInterface
from tabulate import tabulate

def fCarregarTabelas():
    safra = selic = credito = dolar = clima = None

    def fSetSafra(df):
        nonlocal safra
        safra = df.with_columns([
            pl.col("nrAno").cast(pl.Int32),
            pl.col("nrMes").cast(pl.Int32)
        ])

    def fSetSelic(df):
        nonlocal selic
        selic = df.with_columns([
            pl.col("data").dt.year().cast(pl.Int32).alias("nrAno"),
            pl.col("data").dt.month().cast(pl.Int32).alias("nrMes")
        ])

    def fSetCredito(df):
        nonlocal credito
        credito = df.with_columns([
            pl.col("data").dt.year().cast(pl.Int32).alias("nrAno"),
            pl.col("data").dt.month().cast(pl.Int32).alias("nrMes")
        ])

    def fSetDolar(df):
        nonlocal dolar
        dolar = df.with_columns([
            pl.col("data").dt.year().cast(pl.Int32).alias("nrAno"),
            pl.col("data").dt.month().cast(pl.Int32).alias("nrMes")
        ])

    def fSetClima(df):
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

def fAgregaClima4Meses(clima: pl.DataFrame, safra: pl.DataFrame, produto: str) -> pl.DataFrame:
    produtoSafra = safra.filter(pl.col("produto") == produto).select("nrAno", "nrMes").unique()
    clima = clima.with_columns([
        pl.col("data").dt.year().alias("ano"),
        pl.col("data").dt.month().alias("mes")
    ])

    resultados: list[pl.DataFrame] = []
    for row in produtoSafra.iter_rows(named=True):
        ano = row["nrAno"]
        mes = row["nrMes"]
        mesInicio = mes - 4
        anoInicio = ano
        if mesInicio <= 0:
            mesInicio += 12
            anoInicio -= 1

        if anoInicio < clima["ano"].min():
            continue

        climaPeriodo = clima.filter(
            (pl.col("data") >= date(anoInicio, mesInicio, 1)) &
            (pl.col("data") < date(ano, mes, 1))
        )

        if climaPeriodo.height == 0:
            continue

        resumo = climaPeriodo.select([
            pl.lit(ano).alias("nrAno"),
            pl.col("precipitacao").sum().alias("precipitacao"),
            pl.col("pressao").mean().alias("pressao"),
            pl.col("radiacao").mean().alias("radiacao"),
            pl.col("temp_bulbo_seco").mean().alias("temp_bulbo_seco"),
            pl.col("umidade").mean().alias("umidade"),
            pl.col("vento_vel").mean().alias("vento_vel")
        ])

        resultados.append(resumo)

    return pl.concat(resultados) if resultados else pl.DataFrame()

def fMontarBase(safra, selic, credito, dolar, clima, produto):
    produtoDf = safra.filter(pl.col("produto") == produto)
    climaAno = fAgregaClima4Meses(clima, safra, produto)

    if climaAno.is_empty() or "nrAno" not in climaAno.columns:
        return pl.DataFrame()  # <- evita erro de join

    dolarAno = dolar.group_by("nrAno").agg([
        pl.col("cotacao_venda").mean().alias("dolar")
    ])
    selicAno = selic.group_by("nrAno").agg([
        pl.col("valor").mean().alias("selic")
    ])
    creditoAno = credito.group_by("nrAno").agg([
        pl.col("vl_comercializacao").sum().alias("comercializacao"),
        pl.col("vl_custeio").sum().alias("custeio"),
        pl.col("vl_investimento").sum().alias("investimento"),
        (pl.col("vl_comercializacao") + pl.col("vl_custeio") + pl.col("vl_investimento")).sum().alias("valorTotal")
    ])

    base = produtoDf.join(climaAno, on="nrAno", how="inner")
    base = base.join(dolarAno, on="nrAno", how="inner")
    base = base.join(selicAno, on="nrAno", how="inner")
    base = base.join(creditoAno, on="nrAno", how="inner")

    return base

def fPrepararFeaturesELabel(df: pl.DataFrame, alvo: str = "producao") -> tuple[np.ndarray, np.ndarray, StandardScaler, StandardScaler]:
    colunasAlvo = alvo
    colunasNumericas = [
        "comercializacao", "custeio", "investimento", "valorTotal",
        "precipitacao", "pressao", "radiacao",
        "temp_bulbo_seco", "umidade", "vento_vel",
        "selic", "dolar"
    ]

    dfNumerico = df.select(colunasNumericas + [colunasAlvo]).drop_nulls()

    # 🔧 Corrigir valores prováveis em quilos (1000x a mais que deveriam)
    mediana = dfNumerico.select(pl.median(colunasAlvo)).item()
    dfNumerico = dfNumerico.with_columns(
        pl.when(pl.col(colunasAlvo) > mediana * 50)
        .then(pl.col(colunasAlvo) / 1000)
        .otherwise(pl.col(colunasAlvo))
        .alias(colunasAlvo)
    )

    # 📉 Remoção de outliers por IQR
    descr = dfNumerico.select(colunasAlvo).describe()
    q1 = descr.filter(pl.col("statistic") == "25%").select(colunasAlvo).item()
    q3 = descr.filter(pl.col("statistic") == "75%").select(colunasAlvo).item()
    iqr = q3 - q1
    limiteInferior = q1 - 1.5 * iqr
    limiteSuperior = q3 + 1.5 * iqr

    dfFiltrado = dfNumerico.filter(
        (pl.col(colunasAlvo) >= limiteInferior) & (pl.col(colunasAlvo) <= limiteSuperior)
    )

    print(f"\n📊 Descrição da variável alvo ({colunasAlvo}) após limpeza e remoção de outliers:")
    print(dfFiltrado.select(colunasAlvo).describe())
    print("\U0001F522 Número de registros para treino:", dfFiltrado.shape[0])

    scalerX = StandardScaler()
    scalerY = StandardScaler()

    X = scalerX.fit_transform(dfFiltrado.select(colunasNumericas).to_numpy())
    y = scalerY.fit_transform(dfFiltrado[colunasAlvo].to_numpy().reshape(-1, 1)).flatten()

    return X, y, scalerX, scalerY

class MLP(nn.Module):
    def __init__(self, input_dim):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(128, 128),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(128, 1)
        )

    def forward(self, x):
        return self.net(x)

def fTreinarModeloTorchRetornandoMetricas(X, y) -> tuple[float, float]:
    X_treino, X_teste, y_treino, y_teste = train_test_split(X, y, test_size=0.2, random_state=42)
    X_treino = torch.tensor(X_treino, dtype=torch.float32).cuda()
    y_treino = torch.tensor(y_treino, dtype=torch.float32).view(-1, 1).cuda()
    X_teste = torch.tensor(X_teste, dtype=torch.float32).cuda()
    y_teste = torch.tensor(y_teste, dtype=torch.float32).view(-1, 1).cuda()

    model = MLP(X.shape[1]).cuda()
    optimizer = optim.Adam(model.parameters(), lr=0.0005)
    criterion = nn.MSELoss()

    best_loss = float('inf')
    patience = 600  # <- MAIS PACIENTE
    trigger_times = 0

    for epoch in range(5000):
        model.train()
        optimizer.zero_grad()
        y_pred = model(X_treino)
        loss = criterion(y_pred, y_treino)
        loss.backward()
        optimizer.step()

        if epoch % 50 == 0:
            print(f"✅ Epoch {epoch}: Loss = {loss.item():.4f}")

        if loss.item() < best_loss:
            best_loss = loss.item()
            trigger_times = 0
        else:
            trigger_times += 1
            if trigger_times >= patience:
                print(f"⏹️ Early stopping at epoch {epoch}")
                break

    model.eval()
    with torch.no_grad():
        pred = model(X_teste).cpu().numpy()
        y_teste_cpu = y_teste.cpu().numpy()
        mse = mean_squared_error(y_teste_cpu, pred)
        r2 = r2_score(y_teste_cpu, pred)

    print("\n\U0001F4CA Avaliação do Modelo com PyTorch")
    print(f"MSE: {mse}")
    print(f"R²: {r2}")
    return float(mse), float(r2)

def fExecutarPrevisaoProdutoCompleta(produto: str, safra, selic, credito, dolar, clima) -> tuple[str, float, float]:
    print(f"\n\U0001F4E6 Produto selecionado: {produto}\n")
    base = fMontarBase(safra, selic, credito, dolar, clima, produto)

    if base.is_empty() or "precipitacao" not in base.columns:
        print(f"⚠️ Base inválida para: {produto}")
        return produto, float("inf"), float("-inf")

    if base.height < 10:
        print("⚠️ Base insuficiente para:", produto)
        return produto, float("inf"), float("-inf")

    try:
        X, y, _, _ = fPrepararFeaturesELabel(base)
        if len(X) < 10:
            return produto, float("inf"), float("-inf")
        mse, r2 = fTreinarModeloTorchRetornandoMetricas(X, y)
        return produto, mse, r2
    except Exception as e:
        print(f"❌ Erro no produto {produto}: {e}")
        return produto, float("inf"), float("-inf")

def fExecutarPrevisaoTodosProdutos():
    safra, selic, credito, dolar, clima = fCarregarTabelas()
    produtosDesejados = {"Soja", "Mamona", "Pitaya", "Palmito"}

    resultados: list[tuple[str, str, float, float]] = []
    variaveisAlvo = ["producao", "nrProdutividade"]

    for produto in produtosDesejados:
        base = fMontarBase(safra, selic, credito, dolar, clima, produto)

        if base.is_empty() or base.height < 10:
            print(f"⚠️ Base insuficiente para: {produto}")
            continue

        for alvo in variaveisAlvo:
            if alvo not in base.columns:
                continue

            try:
                X, y, _, _ = fPrepararFeaturesELabel(base, alvo)
                if len(X) < 10:
                    continue
                mse, r2 = fTreinarModeloTorchRetornandoMetricas(X, y)
                resultados.append((produto, alvo, mse, r2))
            except Exception as e:
                print(f"❌ Erro no produto {produto} | alvo {alvo}: {e}")

    print("\n📊 R² para cada produto com TODAS as variáveis (produção e produtividade):")
    from tabulate import tabulate
    print(tabulate(resultados, headers=["Produto", "Alvo", "MSE", "R²"], tablefmt="grid"))

def fExecutarAnaliseVariavelUnica():
    safra, selic, credito, dolar, clima = fCarregarTabelas()
    produtosDesejados = {"Soja", "Mamona", "Pitaya", "Palmito"}
    variaveisIndependentes = [
        "comercializacao", "custeio", "investimento", "valorTotal",
        "precipitacao", "pressao", "radiacao",
        "temp_bulbo_seco", "umidade", "vento_vel",
        "selic", "dolar"
    ]
    variaveisAlvo = ["producao", "nrProdutividade"]

    produtos = safra.select("produto").unique().filter(pl.col("produto").is_in(produtosDesejados)).to_series().to_list()
    resultados = []

    for produto in produtos:  # type: ignore
        print(f"\n🔍 Produto: {produto}")
        base = fMontarBase(safra, selic, credito, dolar, clima, produto)

        if base.is_empty() or base.height < 10:
            print(f"⚠️ Base insuficiente para: {produto}")
            continue

        for varAlvo in variaveisAlvo:
            for varIndep in variaveisIndependentes:
                if varIndep not in base.columns or varAlvo not in base.columns:
                    continue

                df = base.select([varIndep, varAlvo]).drop_nulls()
                if df.height < 10:
                    continue

                # Remoção de outliers no alvo
                descr = df.select(varAlvo).describe()
                try:
                    q1 = descr.filter(pl.col("statistic") == "25%").select(varAlvo).item()
                    q3 = descr.filter(pl.col("statistic") == "75%").select(varAlvo).item()
                    iqr = q3 - q1
                    limiteInferior = q1 - 1.5 * iqr
                    limiteSuperior = q3 + 1.5 * iqr
                    df = df.filter((pl.col(varAlvo) >= limiteInferior) & (pl.col(varAlvo) <= limiteSuperior))
                except:
                    continue

                if df.height < 10:
                    continue

                X = df.select(varIndep).to_numpy()
                y = df.select(varAlvo).to_numpy().flatten()

                scalerX = StandardScaler()
                scalerY = StandardScaler()
                X = scalerX.fit_transform(X)
                y = scalerY.fit_transform(y.reshape(-1, 1)).flatten()

                try:
                    mse, r2 = fTreinarModeloTorchRetornandoMetricas(X, y)
                    resultados.append((produto, varIndep, varAlvo, r2, mse))
                except Exception as e:
                    print(f"Erro em {produto} | {varIndep} -> {varAlvo}: {e}")

    # Organizar resultados em tabela
    tabela = []
    for produto in produtos: # type: ignore
        for varIndep in variaveisIndependentes:
            r2Prod = next((r2 for p, vi, va, r2, _ in resultados if p == produto and vi == varIndep and va == "producao"), None)
            r2Produtiv = next((r2 for p, vi, va, r2, _ in resultados if p == produto and vi == varIndep and va == "nrProdutividade"), None)
            tabela.append([produto, varIndep, r2Prod if r2Prod is not None else "-", r2Produtiv if r2Produtiv is not None else "-"])

    print("\n📊 Comparativo R² por variável independente e alvo (produção x produtividade):")
    print(tabulate(tabela, headers=["Produto", "Variável", "R² Produção", "R² Produtividade"], tablefmt="grid"))

def fSalvarModelo(model, scalerX, scalerY, nomeBase="soja_nrProdutividade"):
    torch.save(model.state_dict(), f"{nomeBase}_model.pth")
    torch.save(scalerX, f"{nomeBase}_scalerX.pth")
    torch.save(scalerY, f"{nomeBase}_scalerY.pth")
    print(f"✅ Modelo e scalers salvos como: {nomeBase}_*.pth")

def fCarregarModelo(input_dim, nomeBase="soja_nrProdutividade"):
    model = MLP(input_dim)
    model.load_state_dict(torch.load(f"{nomeBase}_model.pth"))
    model.eval()
    scalerX = torch.load(f"{nomeBase}_scalerX.pth")
    scalerY = torch.load(f"{nomeBase}_scalerY.pth")
    return model, scalerX, scalerY

def fPreverProdutividade2024(safra, selic, credito, dolar, clima):
    produto = "Soja"
    base = fMontarBase(safra, selic, credito, dolar, clima, produto)
    baseTreino = base.filter(pl.col("nrAno") < 2024)
    base2024 = base.filter(pl.col("nrAno") == 2024)

    if base2024.is_empty():
        print("⚠️ Nenhum dado disponível para 2024.")
        return

    X_treino, y_treino, scalerX, scalerY = fPrepararFeaturesELabel(baseTreino, alvo="nrProdutividade")
    model = MLP(X_treino.shape[1]).cuda()
    optimizer = optim.Adam(model.parameters(), lr=0.0005)
    criterion = nn.MSELoss()

    # Treinamento simples
    for epoch in range(2000):
        model.train()
        optimizer.zero_grad()
        y_pred = model(torch.tensor(X_treino, dtype=torch.float32).cuda())
        loss = criterion(y_pred, torch.tensor(y_treino, dtype=torch.float32).view(-1, 1).cuda())
        loss.backward()
        optimizer.step()

    # Salvar modelo treinado
    fSalvarModelo(model, scalerX, scalerY)

    # Previsão para 2024
    X_2024 = base2024.select([
        "comercializacao", "custeio", "investimento", "valorTotal",
        "precipitacao", "pressao", "radiacao",
        "temp_bulbo_seco", "umidade", "vento_vel",
        "selic", "dolar"
    ]).to_numpy()
    X_2024_scaled = scalerX.transform(X_2024)
    X_2024_tensor = torch.tensor(X_2024_scaled, dtype=torch.float32).cuda()

    with torch.no_grad():
        y_pred_scaled = model(X_2024_tensor).cpu().numpy().flatten()
        y_pred_real = scalerY.inverse_transform(y_pred_scaled.reshape(-1, 1)).flatten()
        base2024 = base2024.with_columns(pl.Series(name="produtividade_prevista", values=y_pred_real.tolist()))
        print(base2024.select(["nrAno", "nrMes", "produtividade_prevista"]))

def fExecutarModeloSojaProducaoAte2023():
    safra, selic, credito, dolar, clima = fCarregarTabelas()
    produto = "Soja"
    alvo = "producao"

    print(f"\n🚜 Treinando modelo de produção para: {produto}")
    base = fMontarBase(safra, selic, credito, dolar, clima, produto)
    baseTreino = base.filter(pl.col("nrAno") < 2024)

    if baseTreino.is_empty() or baseTreino.height < 10:
        print("⚠️ Base insuficiente para treino.")
        return

    try:
        X, y, scalerX, scalerY = fPrepararFeaturesELabel(baseTreino, alvo=alvo)

        if len(X) < 10:
            print("⚠️ Dados insuficientes após limpeza.")
            return

        # Criar e treinar modelo
        model = MLP(X.shape[1]).cuda()
        optimizer = optim.Adam(model.parameters(), lr=0.0005)
        criterion = nn.MSELoss()

        best_loss = float("inf")
        trigger_times = 0
        patience = 600

        for epoch in range(5000):
            model.train()
            optimizer.zero_grad()
            y_pred = model(torch.tensor(X, dtype=torch.float32).cuda())
            loss = criterion(y_pred, torch.tensor(y, dtype=torch.float32).view(-1, 1).cuda())
            loss.backward()
            optimizer.step()

            if loss.item() < best_loss:
                best_loss = loss.item()
                trigger_times = 0
            else:
                trigger_times += 1
                if trigger_times >= patience:
                    print(f"⏹️ Early stopping at epoch {epoch}")
                    break

        # Salvar modelo
        fSalvarModelo(model, scalerX, scalerY, nomeBase="soja_producao")

    except Exception as e:
        print(f"❌ Erro durante o treinamento do modelo de produção da soja: {e}")

def fPreverProdutividadeSoja2024():
    from emater_data_science.data.data_interface import DataInterface

    safra, selic, credito, dolar, clima = fCarregarTabelas()
    produto = "Soja"
    alvo = "nrProdutividade"
    nomeBase = "soja_nrProdutividade"
    caminho = "C:/emater_modelos/"  # ajuste se estiver usando outro diretório

    base = fMontarBase(safra, selic, credito, dolar, clima, produto)
    base2024 = base.filter(pl.col("nrAno") == 2024)

    if base2024.is_empty():
        print("⚠️ Nenhum dado disponível para previsão em 2024.")
        return

    # Carregar modelo e scalers
    input_dim = 13  # número de variáveis explicativas
    model = MLP(input_dim)
    model.load_state_dict(torch.load(f"{caminho}{nomeBase}_model.pth"))
    model.eval().cuda()

    scalerX = torch.load(f"{caminho}{nomeBase}_scalerX.pth")
    scalerY = torch.load(f"{caminho}{nomeBase}_scalerY.pth")

    # Selecionar variáveis de entrada
    X_2024 = base2024.select([
        "comercializacao", "custeio", "investimento", "valorTotal",
        "precipitacao", "pressao", "radiacao",
        "temp_bulbo_seco", "umidade", "vento_vel",
        "selic", "dolar"
    ]).to_numpy()

    # Aplicar scaler
    X_scaled = scalerX.transform(X_2024)
    X_tensor = torch.tensor(X_scaled, dtype=torch.float32).cuda()

    # Fazer a previsão
    with torch.no_grad():
        y_pred_scaled = model(X_tensor).cpu().numpy().flatten()
        y_pred_real = scalerY.inverse_transform(y_pred_scaled.reshape(-1, 1)).flatten()

    # Mostrar resultado
    resultado = base2024.select(["nrAno", "nrMes"]).with_columns(
        pl.Series(name="produtividade_prevista", values=y_pred_real.tolist())
    )
    print("\n📈 Previsão de produtividade da soja para 2024:")
    print(resultado)

    return resultado

if __name__ == "__main__":
    fPreverProdutividadeSoja2024()
    DataInterface().fShutdown()
