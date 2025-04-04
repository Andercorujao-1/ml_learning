import asyncio
from datetime import date, time
import aiohttp
from pathlib import Path
import os
from zipfile import ZipFile
from io import  StringIO
import polars as pl
from typing import Final, cast, Any
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# ---------------------------
# SQLAlchemy Base and Model
# ---------------------------
class Base(DeclarativeBase):
    pass

class EstacaoInmetComDadosMeteorologicos(Base):
    __tablename__ = "estacao_inmet_com_dados_meteorologicos"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    data: Mapped[date]  # Mandatory
    hora: Mapped[time]  # Mandatory
    data_fundacao: Mapped[date]

    precipitacao: Mapped[float | None] = mapped_column(nullable=True)
    pressao: Mapped[float | None] = mapped_column(nullable=True)
    pressao_max: Mapped[float | None] = mapped_column(nullable=True)
    pressao_min: Mapped[float | None] = mapped_column(nullable=True)
    radiacao: Mapped[float | None] = mapped_column(nullable=True)
    temp_bulbo_seco: Mapped[float | None] = mapped_column(nullable=True)
    temp_orvalho: Mapped[float | None] = mapped_column(nullable=True)
    temp_max_ant: Mapped[float | None] = mapped_column(nullable=True)
    temp_min_ant: Mapped[float | None] = mapped_column(nullable=True)
    orvalho_max_ant: Mapped[float | None] = mapped_column(nullable=True)
    orvalho_min_ant: Mapped[float | None] = mapped_column(nullable=True)
    umidade_max_ant: Mapped[float | None] = mapped_column(nullable=True)
    umidade_min_ant: Mapped[float | None] = mapped_column(nullable=True)
    umidade: Mapped[float | None] = mapped_column(nullable=True)
    vento_dir: Mapped[float | None] = mapped_column(nullable=True)
    vento_rajada: Mapped[float | None] = mapped_column(nullable=True)
    vento_vel: Mapped[float | None] = mapped_column(nullable=True)

    # Station metadata.
    estacao: Mapped[str]  # Mandatory
    codigo: Mapped[str | None] = mapped_column(nullable=True)
    latitude: Mapped[float | None] = mapped_column(nullable=True)
    longitude: Mapped[float | None] = mapped_column(nullable=True)
    altitude: Mapped[float | None] = mapped_column(nullable=True)

# ---------------------------
# Helper Functions
# ---------------------------
def extract_metadata(lines: list[str]) -> dict[str, Any]:
    """Extracts metadata from the first few lines of the file."""
    try:
        metadata = {
            "regiao": lines[0].split(":")[1].strip(),
            "uf": lines[1].split(":")[1].strip(),
            "estacao": lines[2].split(":")[1].strip(),
            "codigo": lines[3].split(":")[1].strip(),
            "latitude": float(lines[4].split(":")[1].replace(",", ".").replace(";", "").strip()),
            "longitude": float(lines[5].split(":")[1].replace(",", ".").replace(";", "").strip()),
            "altitude": float(lines[6].split(":")[1].replace(",", ".").replace(";", "").strip()),
            "data_fundacao": lines[7].split(":")[1].strip().replace(";", ""),
        }
    except:
        print("Error extracting metadata from the file.")
        print("Available lines:", lines[:9])
        raise

    
    return metadata

def read_and_prepare_dataframe(lines: list[str], metadata: dict[str, Any]) -> pl.DataFrame:
    # Define the possible header names
    possible_headers = ["Data", "DATA (YYYY-MM-DD)"]
    data_start = None
    for i, line in enumerate(lines):
        if any(line.startswith(header) for header in possible_headers):
            data_start = i
            break
    if data_start is None:
        raise ValueError(f"CSV header not found. Expected a header starting with one of: {possible_headers}")

    csv_content = "\n".join(lines[data_start:])
    df = pl.read_csv(
        StringIO(csv_content),
        separator=";",
        encoding="latin1",
        infer_schema_length=10_000,
    )
    
    df = df.with_columns([
        pl.lit(metadata["estacao"]).alias("estacao"),
        pl.lit(metadata["codigo"]).alias("codigo"),
        pl.lit(metadata["uf"]).alias("uf"),
        pl.lit(metadata["regiao"]).alias("regiao"),
        pl.lit(metadata["latitude"]).alias("latitude"),
        pl.lit(metadata["longitude"]).alias("longitude"),
        pl.lit(metadata["altitude"]).alias("altitude"),
        pl.lit(metadata["data_fundacao"]).alias("data_fundacao"),
    ])
    df = df.select([col for col in df.columns if col.strip()])
    
    return df

def get_expected_schema() -> dict[str, pl.DataType]:
    """
    Returns a mapping of expected column names (as in the CSV) to Polars data types.
    Keys must match the CSV header names exactly.
    """
    schema = cast(dict[str, pl.DataType], {
        "data": pl.Utf8,
        "hora": pl.Utf8,
        "data_fundacao": pl.Utf8,
        "precipitacao": pl.Float64,
        "pressao": pl.Float64,
        "pressao_max": pl.Float64,
        "pressao_min": pl.Float64,
        "radiacao": pl.Float64,
        "temp_bulbo_seco": pl.Float64,
        "temp_orvalho": pl.Float64,
        "temp_max_ant": pl.Float64,
        "temp_min_ant": pl.Float64,
        "orvalho_max_ant": pl.Float64,
        "orvalho_min_ant": pl.Float64,
        "umidade_max_ant": pl.Float64,
        "umidade_min_ant": pl.Float64,
        "umidade": pl.Float64,
        "vento_dir": pl.Float64,
        "vento_rajada": pl.Float64,
        "vento_vel": pl.Float64,
        "estacao": pl.Utf8,
        "codigo": pl.Utf8,
        "uf": pl.Utf8,
        "regiao": pl.Utf8,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
        "altitude": pl.Float64,
    })
   
    return schema

def enforce_schema_and_parse(df: pl.DataFrame) -> pl.DataFrame:
    """
    Replaces commas with dots in numeric columns (if needed) and casts
    columns according to the expected schema. Also parses date/time strings.
    """
    schema = get_expected_schema()
    numeric_types = {pl.Float64, pl.Float32, pl.Int64, pl.Int32}
    numeric_cols = [col for col, dtype in schema.items() if dtype in numeric_types]
    
    # For each numeric column with string type, replace commas with dots.
    for col in numeric_cols:
        if col in df.columns and df[col].dtype == pl.Utf8:
            df = df.with_columns(pl.col(col).str.replace_all(",", ".").alias(col))
            
    # Cast all columns to the expected type.
    try:
        df = df.with_columns([pl.col(col).cast(dtype) for col, dtype in schema.items()])
    except:
        print("error enforce_schema_and_parse")
        print("Available columns:", df.columns)
        raise

    # Parse the date column.
    try:
        # First, try with "/" delimiter.
        df = df.with_columns(
            pl.col("data").str.strptime(pl.Date, format="%Y/%m/%d").alias("data")
        )
    except pl.exceptions.InvalidOperationError:
        # If that fails, try with "-" delimiter.
        try:
            df = df.with_columns(
                pl.col("data").str.strptime(pl.Date, format="%Y-%m-%d").alias("data")
            )
        except pl.exceptions.InvalidOperationError as e_date:
            print("Error parsing 'data' with both '/' and '-' formats.")
            print("Available columns:", df.columns)
            raise e_date
    try:
        # First, try with "/" delimiter.
        df = df.with_columns(
            pl.col("data_fundacao").str.strptime(pl.Date, format="%d/%m/%y").alias("data_fundacao")
        )
    except pl.exceptions.InvalidOperationError:
        # If that fails, try with "-" delimiter.
        try:
            df = df.with_columns(
                pl.col("data_fundacao").str.strptime(pl.Date, format="%Y-%m-%d").alias("data_fundacao")
            )
        except pl.exceptions.InvalidOperationError as e_data_fundacao:
            print("Error parsing 'data_fundacao' with both '/' and '-' formats.")
            print("Available columns:", df.columns)
            raise e_data_fundacao

    # Parse the time column.
    try:
        # First, try with colon format.
        df = df.with_columns(
            pl.col("hora")
              .str.replace(" UTC", "")
              .str.strptime(pl.Time, format="%H:%M")
              .alias("hora")
        )
    except pl.exceptions.InvalidOperationError:
        try:
            # If that fails, try without the colon.
            df = df.with_columns(
                pl.col("hora")
                  .str.replace(" UTC", "")
                  .str.strptime(pl.Time, format="%H%M")
                  .alias("hora")
            )
        except pl.exceptions.InvalidOperationError as e_time:
            print("Error parsing 'hora' with both '%H:%M' and '%H%M' formats.")
            print("Available 'hora' values:", df["hora"].to_list())
            raise e_time

    return df

def clean_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Drops rows only if both 'data' and 'hora' are null.
    For all string (pl.Utf8) columns, if a value starts with ';', remove it.
    All other columns may retain null values.
    """
    # Drop rows only if both 'data' and 'hora' are null
    df = df.filter(~(pl.col("data").is_null() & pl.col("hora").is_null()))
    
    # For every Utf8 (string) column, remove any leading ';'
    for col in df.schema:
        if df.schema[col] == pl.Utf8:
            df = df.with_columns(
                pl.when(pl.col(col).str.starts_with(";"))
                .then(pl.col(col).str.strip_prefix(";"))
                .otherwise(pl.col(col))
                .alias(col)
            )
            
    return df

def rename_columns_to_model(df: pl.DataFrame) -> pl.DataFrame:
    """
    Renames CSV header columns to match the SQLAlchemy model attribute names.
    Handles alternate header names like "Data" and "DATA (YYYY-MM-DD)".
    """
    rename_map = {
        # Date column can be "Data" or "DATA (YYYY-MM-DD)"
        "Data": "data",
        "DATA (YYYY-MM-DD)": "data",
        # Time column can be "Hora UTC" or "HORA (UTC)"
        "Hora UTC": "hora",
        "HORA (UTC)": "hora",

        "DATA DE FUNDACAO:": "data_fundacao",
        "DATA DE FUNDAÇÃO (YYYY-MM-DD):": "data_fundacao",

        "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)": "precipitacao",
        "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)": "pressao",
        "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)": "pressao_max",
        "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)": "pressao_min",
        "RADIACAO GLOBAL (Kj/m²)": "radiacao",
        'RADIACAO GLOBAL (KJ/m²)': "radiacao",
        
        "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)": "temp_bulbo_seco",
        "TEMPERATURA DO PONTO DE ORVALHO (°C)": "temp_orvalho",
        "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)": "temp_max_ant",
        "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)": "temp_min_ant",
        "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)": "orvalho_max_ant",
        "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)": "orvalho_min_ant",
        "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)": "umidade_max_ant",
        "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)": "umidade_min_ant",
        "UMIDADE RELATIVA DO AR, HORARIA (%)": "umidade",
        "VENTO, DIREÇÃO HORARIA (gr) (° (gr))": "vento_dir",
        "VENTO, RAJADA MAXIMA (m/s)": "vento_rajada",
        "VENTO, VELOCIDADE HORARIA (m/s)": "vento_vel",
        "estacao": "estacao",
        "codigo": "codigo",
        "uf": "uf",
        "regiao": "regiao",
        "latitude": "latitude",
        "longitude": "longitude",
        "altitude": "altitude",
        
    }
    actual_rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
    return df.rename(actual_rename_map)

def fSaveInmetCsvToDb(textStream: str) -> None:
    # Split file into lines and extract metadata.
    lines = textStream.splitlines()
    metadata = extract_metadata(lines)
    
    # Read CSV content and append metadata.
    df1 = read_and_prepare_dataframe(lines, metadata)
    
    # First, rename columns to your standardized model names.
    df2 = rename_columns_to_model(df1)
    
    # Now that columns match the expected schema keys, enforce the schema and parse date/time.
    df3 = enforce_schema_and_parse(df2)
    
    # Clean the DataFrame.
    df4 = clean_dataframe(df3)
    
    # Drop any 'id' column.
    if "id" in df4.columns:
        df4 = df4.drop("id")
    
    df = df4

    if df.height == 0:
        print('NOME DA CIDADE ?????????????????????????')
        print(df1['estacao'].to_list())
        print(df2['estacao'].to_list())
        print(df3['estacao'].to_list())
        print(df4['estacao'].to_list())
        
        print("DataFrame shape:", df.shape)
        print("DataFrame columns:", df.columns)
        print(df.head())
    # Store the DataFrame using the DataInterface.
    from emater_data_science.data.data_interface import DataInterface
    DataInterface().fStoreTable(model=EstacaoInmetComDadosMeteorologicos, data=df)
    
    # Clear memory.
    del df
    import gc
    gc.collect()

def fMgCitiesFoundationYear() -> dict[str, int]:
    """
    Returns a dictionary mapping each MG city/station name to its ano_fundacao (year).
    """
    return {
        "AGUAS VERMELHAS": 2007,
        "A534_AIMORES": 2007,
        "ALMENARA": 2002,
        "ARACUAI": 2017,
        "ARAXA": 2002,
        "BAMBUI": 2016,
        "BARBACENA": 2002,
        "BELO HORIZONTE - CERCADINHO": 2013,
        "_A521_": 2006, #Pampulha
        "BURITIS": 2007,
        "CALDAS": 2006,
        "CAMPINA VERDE": 2006,
        "CAPELINHA": 2007,
        "CARATINGA": 2007,
        "CHAPADA GAUCHA": 2007,
        "CONCEICAO DAS ALAGOAS": 2006,
        "CORONEL PACHECO": 2012,
        "CURVELO": 2006,
        "DIAMANTINA": 2007,
        "DIVINOPOLIS": 2017,
        "DORES DO INDAIA": 2007,
        "ESPINOSA": 2007,
        "FLORESTAL": 2008,
        "FORMIGA": 2006,
        "GOVERNADOR VALADARES": 2007,
        "GUANHAES": 2007,
        "GUARDA-MOR": 2007,
        "IBIRITE (ROLA MOCA)": 2008,
        "ITAOBIM": 2007,
        "ITUIUTABA": 2006,
        "JANUARIA": 2016,
        "JOAO PINHEIRO": 2007,
        "JUIZ DE FORA": 2007,
        "MACHADO": 2017,
        "MANHUACU": 2010,
        "MANTENA": 2007,
        "MARIA DA FE": 2006,
        "MOCAMBINHO": 2007,
        "MONTALVANIA": 2007,
        "MONTE VERDE": 2004,
        "MONTES CLAROS": 2002,
        "MURIAE": 2006,
        "_A563_": 2016,
        "OLIVEIRA": 2017,
        "OURO BRANCO": 2006,
        "PARACATU": 2018,
        "PASSA QUATRO": 2007,
        "PASSOS": 2006,
        "PATOS DE MINAS": 2017,
        "PATROCINIO": 2006,
        "PIRAPORA": 2007,
        "POMPEU": 2020,
        "RIO PARDO DE MINAS": 2007,
        "SACRAMENTO": 2006,
        "A552_SALINAS": 2007,
        "SAO JOAO DEL REI": 2006,
        "SAO ROMAO": 2007,
        "SAO SEBASTIAO DO PARAISO": 2015,
        "SERRA DOS AIMORES": 2006,
        "SETE LAGOAS": 2016,
        "TEOFILO OTONI": 2006,
        "TIMOTEO": 2006,
        "TRES MARIAS": 2006,
        "UBERABA": 2017,
        "UBERLANDIA": 2002,
        "UNAI": 2007,
        "VARGINHA": 2006,
        "VICOSA": 2005
    }


# ---------------------------
# Main Extraction and Download
# ---------------------------
def fExtractAndSaveInmetCsvsFromZip(year: int) -> None:
    # Wait for the async download function to complete.
    zipPath = asyncio.run(fDownloadInmetZip(year))
    with ZipFile(str(zipPath), "r") as zipFile:
        # Process files inside the zip.
        all_names = zipFile.namelist()
        folder_prefix = f"{year}/"
        if any(name.startswith(folder_prefix) for name in all_names):
            csv_files = [name for name in all_names if name.startswith(folder_prefix) and not name.endswith("/")]
        else:
            csv_files = [name for name in all_names if name.lower().endswith(".csv")]

        # Get the dictionary of MG cities and their foundation years.
        mg_cities = fMgCitiesFoundationYear()  # Expected to return something like: {"CARATINGA": 2007, ...}
        
        # For each city in the dictionary, look for a matching CSV file.
        for mg_name, foundation_year in mg_cities.items():
            # Find CSV files whose basename contains the mg_name substring.
            matching = [
                csv_file for csv_file in csv_files
                if mg_name in os.path.basename(csv_file)
            ]
            # Check if no matching file was found.
            if len(matching) == 0:
                # Only raise an error if the processing year is greater than the foundation year.
                if year > foundation_year:
                    raise Exception(
                        f"File for '{mg_name}' not found in CSV files for year {year} "
                        f"(expected because {year} > foundation year {foundation_year})."
                    )
                else:
                    # It's acceptable if the year equals (or is less than) the foundation year.
                    continue
            # If multiple matching files were found, always raise an error.
            elif len(matching) > 1:
                raise Exception(f"Multiple files found for '{mg_name}': {matching}")
            else:
                csv_file = matching[0]
                with zipFile.open(csv_file) as file:
                    fileContent = file.read().decode("latin1")
                    fSaveInmetCsvToDb(fileContent)

async def fDownloadInmetZip(year: int) -> Path:
    timeout = aiohttp.ClientTimeout(total=600)
    downloadsPath: Final[Path] = Path(os.path.expanduser("~")) / "Downloads"
    downloadsPath.mkdir(parents=True, exist_ok=True)
    destFile: Final[Path] = downloadsPath / f"inmet_bdmep_{year}.zip"
    if destFile.exists():
        
        return destFile
    url = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as response:
            response.raise_for_status()
            with open(destFile, "wb") as f:
                while True:
                    chunk = await response.content.read(1024)
                    if not chunk:
                        break
                    f.write(chunk)
    
    return destFile

# ---------------------------
# Main Execution
# ---------------------------
if __name__ == "__main__":
    
    year = 2000
    while year < 2015:
        fExtractAndSaveInmetCsvsFromZip(year)
        year += 1
    from emater_data_science.data.data_interface import DataInterface
    DataInterface().fShutdown()
    
