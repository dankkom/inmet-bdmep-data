# INMET BDMEP data

Scripts to download and read INMET's BDMEP data.

Data source: https://portal.inmet.gov.br/dadoshistoricos

## fetch.py: Raw files fetcher

Script to fetch raw data files from INMET's BDMEP site.

Usage:

```
usage: run.py [-h] --data-dir DESTDIR years [years ...]

Download INMET BDMEP data

positional arguments:
  years               Years to download

options:
  -h, --help          show this help message and exit
  --data-dir DESTDIR  Destination directory (default: None)
```

## reader

Module to read INMET's BDMEP raw data files.

Example:

```python
from inmet_bdmep.reader import read_zipfile

filepath = "inmet-bdmep_2022_20220712.zip"
df = read_zipfile(filepath)
# 100%|██████████████████████████████████████| 573/573 [01:06<00:00,  8.65it/s]
print(df)
#      hora  precipitacao  pressao_atmosferica  pressao_atmosferica_maxima  pressao_atmosferica_minima  ...  codigo_wmo   latitude  longitude  altitude  data_fundacao
#0        0           0.0                902.9                       902.9                       902.1  ...        A898 -27.388611 -51.215833     963.0     2019-02-15
#1        1           0.0                903.4                       903.4                       902.9  ...        A898 -27.388611 -51.215833     963.0     2019-02-15
#2        2           0.0                903.7                       903.7                       903.3  ...        A898 -27.388611 -51.215833     963.0     2019-02-15
#3        3           0.0                903.4                       903.7                       903.4  ...        A898 -27.388611 -51.215833     963.0     2019-02-15
#4        4           0.0                903.2                       903.4                       903.1  ...        A898 -27.388611 -51.215833     963.0     2019-02-15
#...    ...           ...                  ...                         ...                         ...  ...         ...        ...        ...       ...            ...
#4339    19           0.0                910.3                       910.5                       910.3  ...        A898 -27.388611 -51.215833     963.0     2019-02-15
#4340    20           0.0                910.2                       910.4                       910.1  ...        A898 -27.388611 -51.215833     963.0     2019-02-15
#4341    21           0.0                910.3                       910.3                       910.1  ...        A898 -27.388611 -51.215833     963.0     2019-02-15
#4342    22           0.0                910.4                       910.4                       910.1  ...        A898 -27.388611 -51.215833     963.0     2019-02-15
#4343    23           0.0                910.6                       910.7                       910.4  ...        A898 -27.388611 -51.215833     963.0     2019-02-15
#
#[4327 rows x 29 columns]
```
