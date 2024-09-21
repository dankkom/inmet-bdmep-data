import csv
import datetime as dt
import io
import re
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm


# READ
def columns_renamer(name: str) -> str:
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


def read_metadata(filepath: Path | zipfile.ZipExtFile) -> dict[str, str]:
    if isinstance(filepath, zipfile.ZipExtFile):
        f = io.TextIOWrapper(filepath, encoding="latin-1")
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
        data_fundacao = dt.datetime.strptime(
            data_fundacao,
            "%Y-%m-%d",
        )
    elif re.match("[0-9]{2}/[0-9]{2}/[0-9]{2}", data_fundacao):
        data_fundacao = dt.datetime.strptime(
            data_fundacao,
            "%d/%m/%y",
        )
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


def convert_dates(dates: pd.Series) -> pd.DataFrame:
    dates = dates.str.replace("/", "-")
    return dates


def convert_hours(hours: pd.Series) -> pd.DataFrame:

    def fix_hour_string(hour: str) -> str:
        if re.match(r"^\d{2}\:\d{2}$", hour):
            return hour
        else:
            return hour[:2] + ":00"

    hours = hours.apply(fix_hour_string)
    return hours


def parse_datetime(d: pd.DataFrame) -> pd.DataFrame:
    d = d.assign(
        data_hora=convert_dates(d["data"]) + " " + convert_hours(d["hora"]),
    )
    d = d.assign(
        data_hora=pd.to_datetime(d["data_hora"], format="%Y-%m-%d %H:%M"),
    )
    return d


def read_data(filepath: Path) -> pd.DataFrame:
    d = pd.read_csv(
        filepath,
        sep=";",
        decimal=",",
        na_values="-9999",
        encoding="latin-1",
        skiprows=8,
        usecols=range(19),
    )
    d = d.rename(columns=columns_renamer)
    d = parse_datetime(d)
    d = d.drop(columns=["data", "hora"])
    return d


def read_zipfile(filepath: Path) -> pd.DataFrame:
    data = pd.DataFrame()
    with zipfile.ZipFile(filepath) as z:
        files = [zf for zf in z.infolist() if not zf.is_dir()]
        for zf in tqdm(files):
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
            data = pd.concat((data, d), ignore_index=True)
    return data
