import argparse
import pathlib

from inmet_bdmep import download_year


def get_parser():
    parser = argparse.ArgumentParser(
        description="Download INMET BDMEP data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "years",
        type=int,
        nargs="+",
        help="Years to download",
    )
    parser.add_argument(
        "--destdir",
        type=pathlib.Path,
        required=True,
        help="Destination directory",
    )
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    destdirpath = args.destdir
    years = args.years
    for year in years:
        download_year(year, destdirpath)


if __name__ == "__main__":
    main()
