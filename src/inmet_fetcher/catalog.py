"""INMET BDMEP unified dataset catalog."""

import datetime as dt
from typing import Any

GROUPS = {
    "bdmep": {
        "name": "Banco de Dados Meteorológicos para Ensino e Pesquisa",
    }
}

GROUP_ALIASES: dict[str, list[str]] = {}


def list_datasets(group: str | None = None) -> list[dict[str, Any]]:
    """List available INMET BDMEP datasets for download.

    Args:
        group: Optional group name to filter. If provided and not 'bdmep', returns empty list.

    Returns:
        List of dictionaries containing dataset metadata (id, group, year, url, ext).
    """
    if group is not None and group != "bdmep":
        return []
    current_year = dt.datetime.now().year
    entries = []
    for year in range(2000, current_year + 1):
        entries.append(
            {
                "id": f"bdmep-{year}",
                "group": "bdmep",
                "year": year,
                "url": f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip",
                "ext": "zip",
            }
        )
    return entries
