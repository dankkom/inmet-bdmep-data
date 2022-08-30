

__version__ = "0.0.3"


import datetime as dt
import pathlib

import requests
from tqdm import tqdm


# DOWNLOAD
def parse_last_modified(last_modified: str) -> dt.datetime:
    return dt.datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")


def build_local_filename(year: int, last_modified: dt.datetime) -> str:
    return f"inmet-bdmep_{year}_{last_modified:%Y%m%d}.zip"


def build_url(year):
    return f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"


def download_year(
    year: int,
    destdirpath: pathlib.Path,
    blocksize: int = 2048,
) -> None:

    if not destdirpath.exists():
        destdirpath.mkdir(parents=True)

    url = build_url(year)

    headers = requests.head(url).headers
    last_modified = parse_last_modified(headers["Last-Modified"])
    file_size = int(headers.get("Content-Length", 0))

    destfilename = build_local_filename(year, last_modified)
    destfilepath = destdirpath / destfilename
    if destfilepath.exists():
        return

    r = requests.get(url, stream=True)
    pbar = tqdm(
        desc=f"{year}",
        dynamic_ncols=True,
        leave=True,
        total=file_size,
        unit="iB",
        unit_scale=True,
    )
    with open(destfilepath, "wb") as f:
        for data in r.iter_content(blocksize):
            f.write(data)
            pbar.update(len(data))
    pbar.close()


def cli():

    import argparse

    def expand_years(*years: str) -> list[int]:
        year_list = []
        for y in years:
            if ":" in y:
                start, end = y.split(":")
                year_list.extend(list(range(int(start), int(end) + 1)))
            else:
                year_list.append(int(y))
        return year_list

    def get_args():
        parser = argparse.ArgumentParser(
            description="Download INMET BDMEP data",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "years",
            nargs="+",
            help="Years to download",
        )
        parser.add_argument(
            "--datadir",
            dest="datadir",
            type=pathlib.Path,
            required=True,
            help="Destination directory",
        )
        return parser

    args = get_args()
    datadir = args.datadir
    years = expand_years(*args.years)
    for year in years:
        download_year(year, datadir)


if __name__ == "__main__":
    cli()
