import asyncio
from datetime import date, time, datetime
import aiohttp
from pathlib import Path
import os
from zipfile import ZipFile
from io import TextIOWrapper
import re
import polars as pl
from io import StringIO
from typing import Final
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column



class Base(DeclarativeBase):
    pass

async def fDownloadInmetZip(year: int) -> Path:
    # Define caminho da pasta de downloads
    downloadsPath: Final[Path] = Path(os.path.expanduser("~")) / "Downloads"
    downloadsPath.mkdir(parents=True, exist_ok=True)

    # Nome esperado do arquivo
    destFile: Final[Path] = downloadsPath / f"inmet_bdmep_{year}.zip"

    # Se já existe, retorna diretamente
    if destFile.exists():
        print(f"📦 Arquivo já existe: {destFile}")
        return destFile

    # Caso contrário, baixa o arquivo
    url = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            with open(destFile, "wb") as f:
                while True:
                    chunk = await response.content.read(1024)
                    if not chunk:
                        break
                    f.write(chunk)

    print(f"✅ Arquivo salvo em: {destFile}")
    return destFile

def fExtractAndSaveInmetCsvsFromZip(year: int) -> None:
    # Step 1: Get expected filenames
    expectedFiles = fMgCsvFileNames()

    # Step 2: Download zip
    zipPath = asyncio.run(fDownloadInmetZip(year))

    # Step 3: Extract and process matching CSVs from zip
    with ZipFile(str(zipPath), 'r') as zipFile:
        zipNames = zipFile.namelist()

        for expectedName in expectedFiles:
            # Remove extensão .CSV se existir
            expectedName = expectedName.upper().removesuffix(".CSV")

            # Remove o trecho com datas (_01-01-2022_A_31-12-2022) pra ficar só o padrão base
            baseNameNoYear = re.sub(r"_\d{2}-\d{2}-\d{4}_A_\d{2}-\d{2}-\d{4}$", "", expectedName)

            # Procurar arquivo que contenha esse nome base no ZIP
            matchedFile = next((name for name in zipNames if baseNameNoYear in name.upper()), None)

            if matchedFile is None:
                print(f"[WARN] File not found in zip for pattern: {baseNameNoYear}")
                continue

            # Ler o conteúdo CSV e passar para a função que salva no banco
            with zipFile.open(matchedFile) as csvFile:
                textStream = TextIOWrapper(csvFile, encoding='latin1')
                fSaveInmetCsvToDb(textStream.read())


def fSaveInmetCsvToDb(textStream: str) -> None:
    lines = textStream.splitlines()

    # Metadados
    regiao = lines[0].split(":")[1].strip()
    uf = lines[1].split(":")[1].strip()
    estacao = lines[2].split(":")[1].strip()
    codigo = lines[3].split(":")[1].strip()
    latitude = float(lines[4].split(":")[1].replace(",", ".").replace(";", "").strip())
    longitude = float(lines[5].split(":")[1].replace(",", ".").replace(";", "").strip())
    altitude = float(lines[6].split(":")[1].replace(",", ".").replace(";", "").strip())

    # Pular linhas até encontrar o cabeçalho dos dados (normalmente na linha 8 ou 9)
    dataStartLine = next(i for i, line in enumerate(lines) if line.startswith("Data"))
    csvContent = "\n".join(lines[dataStartLine:])

    df = pl.read_csv(StringIO(csvContent), separator=";", encoding="latin1", infer_schema_length=10_000)

    # Campos fixos
    df = df.with_columns([
        pl.lit(estacao).alias("estacao"),
        pl.lit(codigo).alias("codigo"),
        pl.lit(uf).alias("uf"),
        pl.lit(regiao).alias("regiao"),
        pl.lit(latitude).alias("latitude"),
        pl.lit(longitude).alias("longitude"),
        pl.lit(altitude).alias("altitude"),
    ])
    df = df.select([col for col in df.columns if col.strip()])
    


    # Converter para lista de objetos
    records: list[EstacaoInmetComDadosMeteorologicos] = []
    for row in df.iter_rows(named=True):
        if not row["Data"] or not row["Hora UTC"]:
            continue  # linha claramente vazia

        try:
            def toFloat(value) -> float | None:
                try:
                    return float(str(value).replace(",", ".").replace(";", ""))
                except (ValueError, TypeError):
                    return None
                
            def fParseData(dataStr: str) -> date | None:
                try:
                    return datetime.strptime(dataStr.strip().replace(";", ""), "%d/%m/%Y").date()
                except Exception:
                    return None

            def fParseHora(horaStr: str) -> time | None:
                try:
                    horaStr = horaStr.replace(" UTC", "").strip().replace(";", "")
                    return datetime.strptime(horaStr, "%H%M").time()
                except Exception:
                    return None

            records.append(EstacaoInmetComDadosMeteorologicos(
                data=fParseData(row["Data"]),
                hora=fParseHora(row["Hora UTC"]),

                precipitacao=toFloat(row["PRECIPITAÇÃO TOTAL, HORÁRIO (mm)"]),
                pressao=toFloat(row["PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)"]),
                pressao_max=toFloat(row["PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)"]),
                pressao_min=toFloat(row["PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)"]),
                radiacao=toFloat(row.get("RADIACAO GLOBAL (Kj/m²)", "")),

                temp_bulbo_seco=toFloat(row["TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)"]),
                temp_orvalho=toFloat(row["TEMPERATURA DO PONTO DE ORVALHO (°C)"]),
                temp_max_ant=toFloat(row["TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)"]),
                temp_min_ant=toFloat(row["TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)"]),
                orvalho_max_ant=toFloat(row["TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)"]),
                orvalho_min_ant=toFloat(row["TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)"]),

                umidade_max_ant=toFloat(row["UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)"]),
                umidade_min_ant=toFloat(row["UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)"]),
                umidade=toFloat(row["UMIDADE RELATIVA DO AR, HORARIA (%)"]),

                vento_dir=toFloat(row["VENTO, DIREÇÃO HORARIA (gr) (° (gr))"]),
                vento_rajada=toFloat(row["VENTO, RAJADA MAXIMA (m/s)"]),
                vento_vel=toFloat(row["VENTO, VELOCIDADE HORARIA (m/s)"]),

                estacao=row["estacao"],
                codigo=row["codigo"],
                
                
                latitude=toFloat(row["latitude"]),
                longitude=toFloat(row["longitude"]),
                altitude=toFloat(row["altitude"]),
            ))
        except Exception as e:
            pass
            print(f"[WARN] Ignoring row due to error: {e}")

    if records:
        from emater_data_science.data.data_interface import DataInterface
        DataInterface().fStoreTable(records)


def fMgCsvFileNames() -> list[str]:
    return [
    "INMET_SE_MG_A554_CARATINGA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_F501_BELO HORIZONTE - CERCADINHO_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A514_SAO JOAO DEL REI_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A537_DIAMANTINA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A521_BELO HORIZONTE (PAMPULHA)_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A519_CAMPINA VERDE_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A529_PASSA QUATRO_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A557_CORONEL PACHECO_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A536_DORES DO INDAIA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A540_MANTENA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A539_MOCAMBINHO_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A515_VARGINHA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A507_UBERLANDIA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A506_MONTES CLAROS_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A569_SETE LAGOAS_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A525_SACRAMENTO_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A538_CURVELO_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A550_ITAOBIM_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A565_BAMBUI_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A508_ALMENARA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A568_UBERABA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A523_PATROCINIO_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A528_TRES MARIAS_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A548_CHAPADA GAUCHA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A535_FLORESTAL_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A555_IBIRITE (ROLA MOCA)_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A559_JANUARIA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A512_ITUIUTABA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A545_PIRAPORA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A570_OLIVEIRA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A560_POMPEU_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A520_CONCEICAO DAS ALAGOAS_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A511_TIMOTEO_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A566_ARACUAI_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A556_MANHUACU_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A518_JUIZ DE FORA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A533_GUANHAES_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A542_UNAI_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A552_SALINAS_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A532_GOVERNADOR VALADARES_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A516_PASSOS_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A531_MARIA DA FE_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A502_BARBACENA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A564_DIVINOPOLIS_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A563_NOVA PORTEIRINHA (JANAUBA)_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A509_MONTE VERDE_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A517_MURIAE_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A562_PATOS DE MINAS_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A505_ARAXA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A510_VICOSA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A522_SERRA DOS AIMORES_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A526_MONTALVANIA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A524_FORMIGA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A567_MACHADO_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A571_PARACATU_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A530_CALDAS_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A549_AGUAS VERMELHAS_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A551_RIO PARDO DE MINAS_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A534_AIMORES_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A527_TEOFILO OTONI_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A547_SAO ROMAO_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A541_CAPELINHA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A553_JOAO PINHEIRO_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A513_OURO BRANCO_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A561_SAO SEBASTIAO DO PARAISO_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A543_ESPINOSA_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A544_BURITIS_01-01-2022_A_31-12-2022.CSV",
    "INMET_SE_MG_A546_GUARDA-MOR_01-01-2022_A_31-12-2022.CSV",
]


class EstacaoInmetComDadosMeteorologicos(Base):
    __tablename__ = 'estacao_inmet_com_dados_meteorologicos'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    data: Mapped[date]
    hora: Mapped[time]

    precipitacao: Mapped[float]
    pressao: Mapped[float]
    pressao_max: Mapped[float]
    pressao_min: Mapped[float]
    radiacao: Mapped[float | None]  # pode estar vazio
    temp_bulbo_seco: Mapped[float]
    temp_orvalho: Mapped[float]
    temp_max_ant: Mapped[float]
    temp_min_ant: Mapped[float]
    orvalho_max_ant: Mapped[float]
    orvalho_min_ant: Mapped[float]
    umidade_max_ant: Mapped[float]
    umidade_min_ant: Mapped[float]
    umidade: Mapped[float]
    vento_dir: Mapped[float]
    vento_rajada: Mapped[float]
    vento_vel: Mapped[float]

    # metadados da estação
    estacao: Mapped[str]
    codigo: Mapped[str]
    latitude: Mapped[float]
    longitude: Mapped[float]
    altitude: Mapped[float]

# Exemplo de uso
if __name__ == "__main__":
    fExtractAndSaveInmetCsvsFromZip(2022)
    from emater_data_science.data.data_interface import DataInterface
    DataInterface().fShutdown()