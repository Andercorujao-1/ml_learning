import polars as pl
import time
from emater_data_science.data.data_interface import DataInterface

def fBuscarCoordenadasCSV() -> pl.DataFrame:
    print("\U0001F4C4 Lendo coordenadas dos municípios do arquivo CSV...")
    df = pl.read_csv(
        "C:/emater_data_science/municipios.csv",
        encoding="utf8",
        separator=",",
        columns=["codigo_ibge", "nome", "latitude", "longitude"]
    )
    return df.with_columns([
        pl.col("nome").str.to_uppercase().alias("municipio")
    ]).select(["municipio", "codigo_ibge", "latitude", "longitude"])

def fNormalizarNomeMunicipio(nome: str) -> str:
    substituicoes = {
        "ITABIRINHA": "ITABIRINHA",
        "OLHOS-D'ÁGUA": "OLHOS D'AGUA",
        "PASSA VINTE": "PASSA-VINTE",
        "SÃO TOMÉ DAS LETRAS": "SAO THOME DAS LETRAS",
    }
    return substituicoes.get(nome, nome)

def fCriarTabelaMunicipios():
    dadosSafra = None

    def fSetSafra(safraDf: pl.DataFrame):
        nonlocal dadosSafra
        dadosSafra = safraDf

    DataInterface().fFetchTable("dados_safra_emater", callback=fSetSafra)

    while dadosSafra is None:
        time.sleep(1)

    municipiosSafra = dadosSafra.select(pl.col("municipio").str.to_uppercase().alias("municipio"))

    # Aplica normalização de nomes usando apply na Series
    municipiosSafra = municipiosSafra.with_columns([
        pl.Series("municipio", [fNormalizarNomeMunicipio(m) for m in municipiosSafra["municipio"]]) # type: ignore
    ]).unique().sort("municipio")

    municipiosIBGE = fBuscarCoordenadasCSV()

    municipiosComCoords = municipiosSafra.join(municipiosIBGE, on="municipio", how="left")

    faltantes = municipiosComCoords.filter(pl.col("latitude").is_null())
    if faltantes.height > 0:
        print("\n\u26A0\uFE0F Municípios não encontrados no CSV:")
        for mun in faltantes["municipio"].to_list(): # type: ignore
            print(mun)

    print(f"\n✅ Municípios com coordenadas: {municipiosComCoords.height - faltantes.height} encontrados")

    return municipiosComCoords

if __name__ == "__main__":
    df = fCriarTabelaMunicipios()
    DataInterface().fShutdown()
