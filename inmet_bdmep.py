

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
def columns_renamer(name):
    name = name.lower()
    if re.match(r"data", name):
        return "data"
    if re.match(r"hora", name):
        return "hora"
    if re.match(r"precipita(ç|c)(ã|a)o", name):
        return "precipitacao"
    if re.match(r"press(ã|a)o atmosf(é|e)rica ao n(í|i)vel", name):
        return "pressao_atmosferica"
    if re.match(r"press(ã|a)o atmosf(é|e)rica m(á|a)x", name):
        return "pressao_atmosferica_maxima"
    if re.match(r"press(ã|a)o atmosf(é|e)rica m(í|i)n", name):
        return "pressao_atmosferica_minima"
    if re.match(r"radia(ç|c)(ã|a)o", name):
        return "radiacao"
    if re.match(r"temperatura do ar", name):
        return "temperatura_ar"
    if re.match(r"temperatura do ponto de orvalho", name):
        return "temperatura_orvalho"
    if re.match(r"temperatura m(á|a)x", name):
        return "temperatura_maxima"
    if re.match(r"temperatura m(í|i)n", name):
        return "temperatura_minima"
    if re.match(r"temperatura orvalho m(á|a)x", name):
        return "temperatura_orvalho_maxima"
    if re.match(r"temperatura orvalho m(í|i)n", name):
        return "temperatura_orvalho_minima"
    if re.match(r"umidade rel\. m(á|a)x", name):
        return "umidade_relativa_maxima"
    if re.match(r"umidade rel\. m(í|i)n", name):
        return "umidade_relativa_minima"
    if re.match(r"umidade relativa do ar", name):
        return "umidade_relativa"
    if re.match(r"vento, dire(ç|c)(ã|a)o", name):
        return "vento_direcao"
    if re.match(r"vento, rajada", name):
        return "vento_rajada"
    if re.match(r"vento, velocidade", name):
        return "vento_velocidade"


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


def convert_dates(s: pd.Series) -> pd.DataFrame:
    datas = s.str.replace("/", "-").str.split("-", expand=True)
    datas = datas.rename(columns={0: "ano", 1: "mes", 2: "dia"})
    datas = datas.apply(lambda x: x.astype(int))
    return datas


def convert_hours(s: pd.Series) -> pd.DataFrame:
    s = s.apply(
        lambda x: x if re.match("\d{2}\:\d{2}", x) else x[:2] + ":" + x[2:]
    )
    horas = s.str.split(":", expand=True)[[0]]
    horas = horas.rename(columns={0: "hora"})
    horas = horas.apply(lambda x: x.astype(int))
    return horas


def read_data(filepath: FilepathZip) -> pd.DataFrame:
    d = pd.read_csv(filepath, sep=";", decimal=",", na_values="-9999",
                    encoding="latin-1", skiprows=8, usecols=range(19))
    d = d.rename(columns=columns_renamer)
    datas = convert_dates(d["data"])
    horas = convert_hours(d["hora"])
    d = d.assign(
        ano=datas["ano"],
        mes=datas["mes"],
        dia=datas["dia"],
        hora=horas["hora"],
    )
    d = d.drop(columns=["data"])
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
            empty_columns = [
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
                "vento_rajada",
                "vento_velocidade",
            ]
            empty_rows = d[empty_columns].isnull().all(axis=1)
            d = d.loc[~empty_rows]
            df = pd.concat((df, d), ignore_index=True)
    return df


def _date_partition(df: pd.DataFrame, level: str = "month") -> pd.DataFrame:
    for year in df["ano"].unique():
        df_year = df.loc[df["ano"] == year, :]
        if level == "year":
            yield df_year, year
            continue
        for month in df_year["mes"].unique():
            df_month = df_year.loc[df["mes"] == month, :]
            if level == "month":
                yield df_month, year, month
                continue
            for day in df_month["dia"].unique():
                df_day = df_month.loc[df["dia"] == day, :]
                yield df_day, year, month, day


def write_parquet(df: pd.DataFrame, dirpath: Filepath) -> None:
    """Write date partitioned parquet files in the `dirpath` directory."""
    if isinstance(dirpath, str):
        dirpath = pathlib.Path(dirpath)
    for df_month, year, month in _date_partition(df, level="month"):
        filename = f"{year:04}-{month:02}.parquet"
        df_month.to_parquet(dirpath / filename)
