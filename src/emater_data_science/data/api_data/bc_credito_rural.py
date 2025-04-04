import requests
import pandas as pd
from datetime import date
import polars as pl
from pathlib import Path
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
import os
from io import StringIO
from emater_data_science.data.data_interface import DataInterface
import gc

def fetch_credit_rural_data(year: int) -> pd.DataFrame:
    """
    Consulta a API do SICOR para obter os dados de Crédito Rural
    filtrados pelo ano de emissão (AnoEmissao) e retorna os dados em CSV.
    
    Parâmetro:
      - year: ano desejado (ex.: 2024)
      
    Retorna:
      - DataFrame com os dados retornados pela API.
    """
    base_url = "https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata/CusteioInvestimentoComercialIndustrialSemFiltros"
    # Constrói a URL exatamente como o exemplo que funcionou
    full_url = f"{base_url}?$format=text%2Fcsv&$filter=AnoEmissao%20eq%20'{year}'"
    response = requests.get(full_url)
    response.raise_for_status()  # Levanta exceção em caso de erro
    # Lê os dados CSV a partir do texto retornado
    df = pd.read_csv(StringIO(response.text))
    return df

def main():
    output_dir = r"C:\emater_data_science"
    os.makedirs(output_dir, exist_ok=True)
    
    start_year = 2000
    end_year = 2024

    for year in range(start_year, end_year + 1):
        print(f"Fetching data for year {year}...")
        try:
            df_year = fetch_credit_rural_data(year)
            if not df_year.empty:
                output_path = os.path.join(output_dir, f"credito_rural_{year}.csv")
                df_year.to_csv(output_path, index=False)
                print(f"Data saved to {output_path}")
            else:
                print(f"No data returned for year {year}.")
        except Exception as e:
            print(f"Error fetching data for year {year}: {e}")

###########################################

class Base(DeclarativeBase):
    pass

class CreditoRural(Base):
    __tablename__ = "credito_rural"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    data: Mapped[date]  # You can derive this from MesEmissao + AnoEmissao
    municipio: Mapped[str]
    cod_munic_ibge: Mapped[int]
    vl_custeio: Mapped[float | None]
    vl_investimento: Mapped[float | None]
    vl_comercializacao: Mapped[float | None]
    vl_industrializacao: Mapped[float | None]
    area_custeio: Mapped[float | None]
    area_investimento: Mapped[float | None]

def fProcessAndSaveCreditoRuralCsv(csvPath: Path) -> None:
    df = pl.read_csv(
        csvPath,
        separator=",",
        encoding="utf8",
        infer_schema_length=5000,
        try_parse_dates=True
    )

    # Filter only MG
    df = df.filter(pl.col("nomeUF") == "MG")

    # Clean up number format and cast
    for col in [
        "VlCusteio", "VlInvestimento", "VlComercializacao", "VlIndustrializacao",
        "AreaCusteio", "AreaInvestimento"
    ]:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).cast(pl.Utf8).str.replace_all(",", ".").cast(pl.Float64)
            )

    # Generate a "data" column from Ano + Mes
    df = df.with_columns(
        pl.date(
            year=pl.col("AnoEmissao").cast(pl.Int32),
            month=pl.col("MesEmissao").cast(pl.Int32),
            day=pl.lit(1)
        ).alias("data")
    )

    df = df.select([
        "data",
        "Municipio",
        "codMunicIbge",
        "VlCusteio",
        "VlInvestimento",
        "VlComercializacao",
        "VlIndustrializacao",
        "AreaCusteio",
        "AreaInvestimento"
    ]).rename({
        "Municipio": "municipio",
        "codMunicIbge": "cod_munic_ibge",
        "VlCusteio": "vl_custeio",
        "VlInvestimento": "vl_investimento",
        "VlComercializacao": "vl_comercializacao",
        "VlIndustrializacao": "vl_industrializacao",
        "AreaCusteio": "area_custeio",
        "AreaInvestimento": "area_investimento"
    })

    DataInterface().fStoreTable(model=CreditoRural, data=df)

    # Clean up memory
    del df
    gc.collect()

def fLoadAllCreditoRuralCsvs() -> None:
    basePath = Path("C:/emater_data_science")
    for year in range(2013, 2025):
        print(f"Loading crédito rural data for year {year}...")
        filePath = basePath / f"credito_rural_{year}.csv"
        if filePath.exists():
            fProcessAndSaveCreditoRuralCsv(filePath)
        else:
            print(f"File not found: {filePath}")


if __name__ == "__main__":
    fLoadAllCreditoRuralCsvs()
    DataInterface().fShutdown()
