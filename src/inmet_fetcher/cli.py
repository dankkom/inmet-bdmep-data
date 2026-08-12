"""Standalone command-line interface for inmet-fetcher."""

import sys

from .plugin import app


def main(argv: list[str] | None = None) -> None:
    """Run the standalone CLI for inmet-fetcher.

    Args:
        argv: Optional list of command-line arguments to parse. Defaults to sys.argv.
    """
    if argv is not None:
        sys.argv = [sys.argv[0]] + argv
    try:
        app()
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
