from datetime import date
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.orm import DeclarativeBase
import polars as pl
from emater_data_science.data.data_interface import DataInterface
import time
import psutil
import os
import gc
import numpy as np
import math



class Base(DeclarativeBase):
    pass


class EstacaoInmetMeteorologicoDiario(Base):
    __tablename__ = "estacao_inmet_meteorologico_diario"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    data: Mapped[date]
    data_fundacao: Mapped[date]

    precipitacao: Mapped[float | None]
    pressao: Mapped[float | None]
    pressao_max: Mapped[float | None]
    pressao_min: Mapped[float | None]
    radiacao: Mapped[float | None]
    temp_bulbo_seco: Mapped[float | None]
    temp_orvalho: Mapped[float | None]
    temp_max_ant: Mapped[float | None]
    temp_min_ant: Mapped[float | None]
    orvalho_max_ant: Mapped[float | None]
    orvalho_min_ant: Mapped[float | None]
    umidade_max_ant: Mapped[float | None]
    umidade_min_ant: Mapped[float | None]
    umidade: Mapped[float | None]
    vento_dir: Mapped[float | None]
    vento_rajada: Mapped[float | None]
    vento_vel: Mapped[float | None]

    estacao: Mapped[str]
    codigo: Mapped[str | None]
    latitude: Mapped[float | None]
    longitude: Mapped[float | None]
    altitude: Mapped[float | None]


def circular_mean(series: pl.Series) -> float|None:
    """
    Calculate the circular mean of a series of angles in degrees.
    Converts angles to radians, computes the mean using sine and cosine,
    then converts the result back to degrees and normalizes it to [0, 360).
    """
    # Drop null values and convert to a NumPy array
    arr = series.drop_nulls().to_numpy()
    if arr.size == 0:
        return None
    # Convert degrees to radians
    radians = np.deg2rad(arr)
    sin_sum = np.sum(np.sin(radians))
    cos_sum = np.sum(np.cos(radians))
    # Compute the mean angle in radians
    mean_rad = math.atan2(sin_sum, cos_sum)
    # Convert back to degrees
    mean_deg = math.degrees(mean_rad)
    # Normalize to the range [0, 360)
    return mean_deg if mean_deg >= 0 else mean_deg + 360


def fProcessInmetYear(year: int) -> None:

    def _onData(df: pl.DataFrame) -> None:
        if df.is_empty():
            return

        # Ensure 'data' is only the date part if it's datetime
        if df.schema["data"] == pl.Datetime:
            df = df.with_columns(pl.col("data").dt.date().alias("data"))

        # Replace -9999.0 with None for all float columns
        df = df.with_columns([
            pl.when(pl.col(col) == -9999.0).then(None).otherwise(pl.col(col)).alias(col)
            for col in df.columns if df.schema[col] == pl.Float64
        ])

        # Define the grouping keys (e.g., by date, station, and code)
        group_keys = ["data", "estacao", "codigo"]

        # Aggregation logic
        aggregations = []

        # Sum for additive columns (e.g., precipitation is cumulative)
        if "precipitacao" in df.columns:
            aggregations.append(pl.col("precipitacao").sum().alias("precipitacao"))

        # For radiation, decide based on your data's nature:
        if "radiacao" in df.columns:
            # Use sum if radiation is cumulative over the hour:
            aggregations.append(pl.col("radiacao").sum().alias("radiacao"))
            # Alternatively, if it were an instantaneous measure, you might use mean:
            # aggregations.append(pl.col("radiacao").mean().alias("radiacao"))

        # Pressure: average overall pressure, max for daily high, min for daily low
        if "pressao" in df.columns:
            aggregations.append(pl.col("pressao").mean().alias("pressao"))
        if "pressao_max" in df.columns:
            aggregations.append(pl.col("pressao_max").max().alias("pressao_max"))
        if "pressao_min" in df.columns:
            aggregations.append(pl.col("pressao_min").min().alias("pressao_min"))

        # Temperature: average for general temperatures; use max/min for extremes
        if "temp_bulbo_seco" in df.columns:
            aggregations.append(pl.col("temp_bulbo_seco").mean().alias("temp_bulbo_seco"))
        if "temp_orvalho" in df.columns:
            aggregations.append(pl.col("temp_orvalho").mean().alias("temp_orvalho"))
        if "temp_max_ant" in df.columns:
            aggregations.append(pl.col("temp_max_ant").max().alias("temp_max_ant"))
        if "temp_min_ant" in df.columns:
            aggregations.append(pl.col("temp_min_ant").min().alias("temp_min_ant"))

        # Dew point (orvalho): use max/min to capture extremes
        if "orvalho_max_ant" in df.columns:
            aggregations.append(pl.col("orvalho_max_ant").max().alias("orvalho_max_ant"))
        if "orvalho_min_ant" in df.columns:
            aggregations.append(pl.col("orvalho_min_ant").min().alias("orvalho_min_ant"))

        # Humidity: average overall humidity; max/min for extremes
        if "umidade_max_ant" in df.columns:
            aggregations.append(pl.col("umidade_max_ant").max().alias("umidade_max_ant"))
        if "umidade_min_ant" in df.columns:
            aggregations.append(pl.col("umidade_min_ant").min().alias("umidade_min_ant"))
        if "umidade" in df.columns:
            aggregations.append(pl.col("umidade").mean().alias("umidade"))

        if "vento_rajada" in df.columns:
            aggregations.append(pl.col("vento_rajada").max().alias("vento_rajada"))
        if "vento_vel" in df.columns:
            aggregations.append(pl.col("vento_vel").mean().alias("vento_vel"))

        # Meta columns (using first non-null value, assuming they don't change within a group)
        meta_cols = ["data_fundacao", "latitude", "longitude", "altitude"]
        for col in meta_cols:
            if col in df.columns:
                aggregations.append(pl.col(col).drop_nulls().first().alias(col))

        dfGrouped = df.group_by(group_keys).agg(aggregations)

        # Step 1: Aggregate all other fields first (like you already do)
        if "vento_dir" in df.columns:
            ventoCircular = (
                df.group_by(group_keys)
                .agg(pl.col("vento_dir").alias("vento_dir_list"))
                .with_columns(
                    pl.col("vento_dir_list")
                    .map_elements(circular_mean, return_dtype=pl.Float64)
                    .alias("vento_dir")
                )
                .drop("vento_dir_list")
            )
            dfGrouped = dfGrouped.join(ventoCircular, on=group_keys, how="left")


        # Store the aggregated data
        DataInterface().fStoreTable(model=EstacaoInmetMeteorologicoDiario, data=dfGrouped)
        print("stored")

        mem = psutil.Process(os.getpid()).memory_info().rss / 1024**2
        print(f"Memory usage: {mem:.2f} MB")

        del df, dfGrouped
        gc.collect()

    DataInterface().fFetchTable(
        tableName="estacao_inmet_com_dados_meteorologicos",
        callback=_onData,
        dateColumn="data",
        startDate=date(year, 1, 1),
        endDate=date(year, 12, 31)
    )


def fGenerateAllYears(start: int = 2000, end: int = 2024) -> None:
    for year in range(start, end + 1):
        print(f"Processing year {year}...")
        fProcessInmetYear(year)

    while not DataInterface().fQueueIsEmpty():
        time.sleep(1)
    DataInterface().fShutdown()


if __name__ == "__main__":
    fGenerateAllYears(2023, 2024)
