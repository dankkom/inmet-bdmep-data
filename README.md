# INMET BDMEP data

Scripts to download and read INMET's BDMEP data.

Data source: https://portal.inmet.gov.br/dadoshistoricos

## fetch-raw.py: Raw files fetcher

Script to fetch raw data files from INMET's BDMEP site.

Usage:

```
usage: fetch-raw.py [-h] --destdir DESTDIR years [years ...]

Download INMET BDMEP data

positional arguments:
  years              Years to download

options:
  -h, --help         show this help message and exit
  --destdir DESTDIR  Destination directory (default: None)
```

## read-and-export.py: Read and export raw files

Script to convert raw data files into csv or parquet files.

Usage:

```
usage: read-and-export.py [-h] [-o OUTPUT] [--level {year,month,day}] [--format {csv,parquet}] filepath

Reads a zip file containing daily weather data from the Brazilian Meteorological Agency (Brasil) and converts it to a
pandas DataFrame.

positional arguments:
  filepath              Path to the zip file containing the daily weather data.

options:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Path to the output directory.
  --level {year,month,day}
                        The granularity of the output DataFrame.
  --format {csv,parquet}
                        The format of the output file.
```
