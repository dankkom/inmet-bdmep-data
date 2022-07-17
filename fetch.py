import argparse
import pathlib

from inmet_bdmep import download_year


def expand_years(*years: str) -> list[int]:
    year_list = []
    for y in years:
        if ":" in y:
            start, end = y.split(":")
            year_list.extend(list(range(int(start), int(end) + 1)))
        else:
            year_list.append(int(y))
    return year_list


def get_parser():
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
        "-o",
        "--output",
        dest="output",
        type=pathlib.Path,
        required=True,
        help="Destination directory",
    )
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    destdirpath = args.output
    years = expand_years(*args.years)
    for year in years:
        download_year(year, destdirpath)


if __name__ == "__main__":
    main()
