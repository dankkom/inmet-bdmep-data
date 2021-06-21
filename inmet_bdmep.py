

__version__ = "0.0.2"


import csv
import datetime
import io
import os
import pathlib
import re
import zipfile
from typing import Dict, Union

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

Filepath = Union[str, pathlib.Path, os.PathLike]
FilepathZip = Union[Filepath, zipfile.ZipExtFile]


# DOWNLOAD
def download_year(
    year: int,
    destpath: Filepath,
    blocksize: int = 2048,
) -> None:
    # Reference https://stackoverflow.com/a/37573701
    if isinstance(destpath, str):
        destpath = pathlib.Path(destpath)
    filename = f"{year}.zip"
    url = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{filename}"
    r = requests.get(url, stream=True)
    file_size = int(r.headers.get("content-length", 0))
    pbar = tqdm(
        desc=filename,
        dynamic_ncols=True,
        leave=True,
        total=file_size,
        unit="iB",
        unit_scale=True,
    )
    with open(destpath / filename, "wb") as f:
        for data in r.iter_content(blocksize):
            f.write(data)
            pbar.update(len(data))
    pbar.close()


# READ
def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "DATA (YYYY-MM-DD)": "data",
            "Data": "data",
            "HORA (UTC)": "hora",
            "Hora UTC": "hora",
            "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)": "precipitacao",
            "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)": "pressao_atmosferica",
            "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)": "pressao_atmosferica_maxima",
            "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)": "pressao_atmosferica_minima",
            "RADIACAO GLOBAL (KJ/m²)": "radiacao",
            "RADIACAO GLOBAL (Kj/m²)": "radiacao",
            "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)": "temperatura_ar",
            "TEMPERATURA DO PONTO DE ORVALHO (°C)": "temperatura_orvalho",
            "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)": "temperatura_maxima",
            "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)": "temperatura_minima",
            "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)": "temperatura_orvalho_maxima",
            "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)": "temperatura_orvalho_minima",
            "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)": "umidade_relativa_maxima",
            "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)": "umidade_relativa_minima",
            "UMIDADE RELATIVA DO AR, HORARIA (%)": "umidade_relativa",
            "VENTO, DIREÇÃO HORARIA (gr) (° (gr))": "vento_direcao",
            "VENTO, RAJADA MAXIMA (m/s)": "vento_rajada_maxima",
            "VENTO, VELOCIDADE HORARIA (m/s)": "vento_velocidade",
        }
    )


def read_metadata(filepath: FilepathZip) -> Dict[str, str]:
    if isinstance(filepath, zipfile.ZipExtFile):
        f = io.TextIOWrapper(filepath)
    else:
        f = open(filepath, "r", encoding="latin-1")
    reader = csv.reader(f, delimiter=";")
    _, regiao = next(reader)
    _, uf = next(reader)
    _, estacao = next(reader)
    _, codigo_wmo = next(reader)
    _, latitude = next(reader)
    try:
        latitude = float(latitude.replace(",", "."))
    except:
        latitude = np.nan
    _, longitude = next(reader)
    try:
        longitude = float(longitude.replace(",", "."))
    except:
        longitude = np.nan
    _, altitude = next(reader)
    try:
        altitude = float(altitude.replace(",", "."))
    except:
        altitude = np.nan
    _, data_fundacao = next(reader)
    if re.match("[0-9]{4}-[0-9]{2}-[0-9]{2}", data_fundacao):
        data_fundacao = datetime.datetime.strptime(
            data_fundacao, "%Y-%m-%d")
    elif re.match("[0-9]{2}/[0-9]{2}/[0-9]{2}", data_fundacao):
        data_fundacao = datetime.datetime.strptime(
            data_fundacao, "%d/%m/%y")
    f.close()
    return {
        "regiao": regiao,
        "uf": uf,
        "estacao": estacao,
        "codigo_wmo": codigo_wmo,
        "latitude": latitude,
        "longitude": longitude,
        "altitude": altitude,
        "data_fundacao": data_fundacao,
    }


def read_data(filepath: FilepathZip) -> pd.DataFrame:
    d = pd.read_csv(filepath, sep=";", decimal=",", na_values="-9999",
                    encoding="latin-1", skiprows=8, usecols=range(19))
    d = rename_columns(d)
    d = d.assign(
        data_hora=pd.to_datetime(d["data"] + " " + d["hora"]),
    )
    d = d.drop(columns=["data", "hora"])
    return d


def read_zipfile(filepath: Filepath) -> pd.DataFrame:
    df = pd.DataFrame()
    with zipfile.ZipFile(filepath) as z:
        for zf in z.infolist():
            if zf.is_dir():
                continue
            d = read_data(z.open(zf.filename))
            meta = read_metadata(z.open(zf.filename))
            d = d.assign(**meta)
            empty_rows = d[
                [
                    "precipitacao",
                    "pressao_atmosferica",
                    "pressao_atmosferica_maxima",
                    "pressao_atmosferica_minima",
                    "radiacao",
                    "temperatura_ar",
                    "temperatura_orvalho",
                    "temperatura_maxima",
                    "temperatura_minima",
                    "temperatura_orvalho_maxima",
                    "temperatura_orvalho_minima",
                    "umidade_relativa_maxima",
                    "umidade_relativa_minima",
                    "umidade_relativa",
                    "vento_direcao",
                    "vento_rajada_maxima",
                    "vento_velocidade",
                ]
            ].isnull().all(axis=1)
            d = d.loc[~empty_rows]
            df = pd.concat((df, d), ignore_index=True)
    return df


def _date_partition(df: pd.DataFrame, level: str = "month") -> pd.DataFrame:
    for year in df["data_hora"].dt.year.unique():
        df_year = df.loc[df["data_hora"].dt.year == year, :]
        if level == "year":
            yield df_year, year
            continue
        for month in df_year["data_hora"].dt.month.unique():
            df_month = df_year.loc[df["data_hora"].dt.month == month, :]
            if level == "month":
                yield df_month, year, month
                continue
            for day in df_month["data_hora"].dt.month.unique():
                df_day = df_month.loc[df["data_hora"].dt.day == day, :]
                yield df_day, year, month, day


def write_csv_zip(df: pd.DataFrame, dirpath: Filepath) -> None:
    """Write date partitioned CSV/ZIP files in the `dirpath` directory."""
    if isinstance(dirpath, str):
        dirpath = pathlib.Path(dirpath)
    for df_month, year, month in _date_partition(df, level="month"):
        filename = f"{year:04}-{month:02}.csv"
        df_month.to_csv(
            dirpath / (filename + ".zip"),
            compression={
                "method": "zip",
                "archive_name": filename,
            },
            encoding="utf-8",
            index=False,
        )
