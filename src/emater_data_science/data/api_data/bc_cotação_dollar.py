import requests
import pandas as pd
from datetime import date, datetime
import polars as pl
from pathlib import Path
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
import os
from io import StringIO
from emater_data_science.data.data_interface import DataInterface
import gc


    

def fetch_dollar_period_csv(start_date: datetime, end_date: datetime):
    """
    Consulta a API do Banco Central para obter a cotação do dólar no período informado,
    retornando os dados no formato CSV.

    As datas devem ser informadas no formato MM-dd-yyyy, envoltas em aspas simples.
    """
    base_url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
    formatted_start = start_date.strftime("%m-%d-%Y")
    formatted_end = end_date.strftime("%m-%d-%Y")
    params = {
        "@dataInicial": f"'{formatted_start}'",  # exemplo: '01-01-2024'
        "@dataFinalCotacao": f"'{formatted_end}'",  # exemplo: '12-31-2024'
        "$format": "text/csv",
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()  # Levanta exceção em caso de erro na requisição
    df = pd.read_csv(StringIO(response.text))
    return df


def main():
    start_year = 2000
    end_year = 2024
    output_dir = r"C:\emater_data_science"
    os.makedirs(output_dir, exist_ok=True)

    for year in range(start_year, end_year + 1):
      
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)
        print(
            f"Coletando dados para {year} (período: {start_date.strftime('%m-%d-%Y')} a {end_date.strftime('%m-%d-%Y')})"
        )
        try:
            df_year = fetch_dollar_period_csv(start_date, end_date)
            if not df_year.empty:
                # Caso exista uma coluna de data (por exemplo, 'dataHoraCotacao'), converte-a para datetime e ordena
                if "dataHoraCotacao" in df_year.columns:
                    df_year["dataHoraCotacao"] = pd.to_datetime(
                        df_year["dataHoraCotacao"]
                    )
                    df_year = df_year.sort_values("dataHoraCotacao")
                output_path = os.path.join(output_dir, f"cotacao_dolar_{year}.csv")
                df_year.to_csv(output_path, index=False)
                print(f"Dados salvos em '{output_path}'")
            else:
                print(f"Nenhum dado retornado para o ano {year}.")
        except Exception as e:
            print(f"Erro ao coletar dados para o ano {year}: {e}")

            ##################################################


class Base(DeclarativeBase):
    pass


class CotacaoDolar(Base):
    __tablename__ = "cotacao_dolar"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    data: Mapped[date]
    cotacao_compra: Mapped[float]
    cotacao_venda: Mapped[float]


def fProcessAndSaveCotacaoDolarCsv(csvPath: Path) -> None:
    df = pl.read_csv(
        csvPath,
        separator=",",
        encoding="utf8",
        infer_schema_length=1000,
    )

    df = df.with_columns(
        [
            pl.col("cotacaoCompra")
            .str.replace_all(",", ".")
            .cast(pl.Float64)
            .alias("cotacao_compra"),

            pl.col("cotacaoVenda")
            .str.replace_all(",", ".")
            .cast(pl.Float64)
            .alias("cotacao_venda"),

            pl.col("dataHoraCotacao")
            .str.split(" ")
            .list.get(0)
            .str.strptime(pl.Date, format="%Y-%m-%d")
            .alias("data")
        ]
    )

    df = df.select(["cotacao_compra", "cotacao_venda", "data"])

    DataInterface().fStoreTable(model=CotacaoDolar, data=df)

    # Clean up memory
    del df
    gc.collect()


def fLoadAllCotacaoDolarCsvs() -> None:
    basePath = Path("C:/emater_data_science")
    for year in range(2000, 2025):
        
        print(f"Loading data for year {year}...")
        filePath = basePath / f"cotacao_dolar_{year}.csv"
        if filePath.exists():
            fProcessAndSaveCotacaoDolarCsv(filePath)
        else:
            print(f"File not found: {filePath}")


if __name__ == "__main__":
    fLoadAllCotacaoDolarCsvs()
    DataInterface().fShutdown()
