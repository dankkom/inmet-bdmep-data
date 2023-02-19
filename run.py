from inmet_bdmep.fetcher import download_year


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
        args = parser.parse_args()
        return args

    args = get_args()
    datadir = args.datadir
    years = expand_years(*args.years)
    for year in years:
        download_year(year, datadir)


if __name__ == "__main__":
    cli()
