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

def fetch_data(start_date, end_date):
    """
    Realiza a requisição para o serviço BCData/SGS para o período definido pelas datas start_date e end_date.
    """
    base_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados"
    params = {
        "formato": "csv",
        "dataInicial": start_date.strftime("%d/%m/%Y"),
        "dataFinal": end_date.strftime("%d/%m/%Y")
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()  # Lança exceção se ocorrer erro na requisição
    df = pd.read_csv(StringIO(response.text), sep=";")
    return df

def main():
    # Define o período de consulta: de 2000 a 2024
    start_year = 2000
    end_year = 2024

    # Define o diretório onde os arquivos CSV serão salvos
    output_dir = r"C:\emater_data_science"
    os.makedirs(output_dir, exist_ok=True)  # Cria o diretório se não existir

    # Itera ano a ano
    for year in range(start_year, end_year + 1):
        current_start = datetime(year, 1, 1)
        current_end = datetime(year, 12, 31)
        print(f"Coletando dados para o ano {year} ({current_start.strftime('%d/%m/%Y')} a {current_end.strftime('%d/%m/%Y')})")
        try:
            df_year = fetch_data(current_start, current_end)
            # Converte a coluna de data e ordena os dados
            df_year['data'] = pd.to_datetime(df_year['data'], dayfirst=True)
            df_year = df_year.sort_values('data')
            
            # Salva o CSV com o nome "taxa_selic_<ano>.csv"
            output_path = os.path.join(output_dir, f"taxa_selic_{year}.csv")
            df_year.to_csv(output_path, index=False)
            print(f"Dados salvos em '{output_path}'")
        except Exception as e:
            print(f"Erro ao coletar dados para o ano {year}: {e}")

##################################################################
class Base(DeclarativeBase):
    pass

class TaxaSelic(Base):
    __tablename__ = "taxa_selic"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    data: Mapped[date]
    valor: Mapped[float]

def fProcessAndSaveTaxaSelicCsv(csvPath: Path) -> None:
    df = pl.read_csv(
        csvPath,
        separator=",",
        encoding="utf8",
        infer_schema_length=1000,
    )

    df = df.with_columns(
        [
            pl.col("data").str.strptime(pl.Date, format="%Y-%m-%d"),
            pl.col("valor").cast(pl.Float64)
        ]
    )

    df = df.select(["data", "valor"])

    DataInterface().fStoreTable(model=TaxaSelic, data=df)

    # Clean up
    del df
    gc.collect()

def fLoadAllTaxaSelicCsvs() -> None:
    basePath = Path("C:/emater_data_science")
    for year in range(2000, 2025):
        print(f"Loading SELIC data for year {year}...")
        filePath = basePath / f"taxa_selic_{year}.csv"
        if filePath.exists():
            fProcessAndSaveTaxaSelicCsv(filePath)
        else:
            print(f"File not found: {filePath}")



if __name__ == "__main__":
    fLoadAllTaxaSelicCsvs()
    DataInterface().fShutdown()