import time
import polars as pl
from emater_data_science.data.data_interface import DataInterface


def fLerCorrelacoesComoMatriz() -> pl.DataFrame:
    resultado = None

    def _onRead(df: pl.DataFrame):
        nonlocal resultado
        # Cria uma coluna descritiva para a variável (ex: "clima: precipitacao_total_dias")
        df = df.with_columns([
            (df["fonte"] + ": " + df["variavel"]).alias("variavelCompleta")
        ])
        # Faz pivot para colocar as variáveis como colunas e produtos como linhas
        resultado = df.pivot(
            on="variavel",                # variáveis que virarão colunas
            index="produto",              # índice (linhas da tabela final)
            values="correlacao",          # valores numéricos
            aggregate_function="first",   # pode ser "mean", "sum" ou qualquer agregação
            sort_columns=True             # opcional, mas deixa mais organizado
        )

    DataInterface().fFetchTable("correlacoes_produtividade", callback=_onRead)

    while resultado is None:
        time.sleep(0.1)

    return resultado


if __name__ == "__main__":
    matriz = fLerCorrelacoesComoMatriz()
    print(matriz)
    DataInterface().fShutdown()
