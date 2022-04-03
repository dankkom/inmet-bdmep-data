import argparse
import pathlib

from inmet_bdmep import (
    _date_partition,
    build_processed_filename,
    read_zipfile,
    write_csv,
    write_parquet,
)


def get_parser():
    parser = argparse.ArgumentParser(
        description="""
        Reads a zip file containing daily weather data from the Brazilian
        Meteorological Agency (Brasil) and converts it to a pandas DataFrame.
        """
    )
    parser.add_argument(
        "filepath",
        type=pathlib.Path,
        help="""
        Path to the zip file containing the daily weather data.
        """
    )
    parser.add_argument(
        "-o",
        "--output",
        type=pathlib.Path,
        default=pathlib.Path("."),
        help="""
        Path to the output directory.
        """
    )
    parser.add_argument(
        "--level",
        type=str,
        default="month",
        choices=["year", "month", "day"],
        help="""
        The granularity of the output DataFrame.
        """
    )
    parser.add_argument(
        "--format",
        type=str,
        default="csv",
        choices=["csv", "parquet"],
        help="""
        The format of the output file.
        """
    )
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    file_format = args.format
    df = read_zipfile(args.filepath)
    for df_part, *leves in _date_partition(df, args.level):
        # Save CSV file for each partition according with the granularity and format
        filename = build_processed_filename(*leves, file_format=file_format)
        filepath = args.output / filename
        print(filepath)
        if file_format == "csv":
            write_csv(df_part, filepath)
        elif file_format == "parquet":
            write_parquet(df_part, filepath)


if __name__ == "__main__":
    main()
