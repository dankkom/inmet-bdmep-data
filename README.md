# INMET BDMEP data

Scripts to download and read INMET's BDMEP data.

Data source: https://portal.inmet.gov.br/dadoshistoricos

## fetch.py: Raw files fetcher

Script to fetch raw data files from INMET's BDMEP site.

Usage:

```
usage: fetch.py [-h] --destdir DESTDIR years [years ...]

Download INMET BDMEP data

positional arguments:
  years              Years to download

options:
  -h, --help         show this help message and exit
  --destdir DESTDIR  Destination directory (default: None)
```
