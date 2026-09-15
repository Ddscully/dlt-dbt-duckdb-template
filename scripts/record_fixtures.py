"""Record the ingest fixtures that CI runs against.

Fetches every source once and writes `tests/fixtures/ingest/`, so CI runs on
real data without depending on any publisher being up.

Run:  uv run python -m scripts.record_fixtures
"""

from __future__ import annotations

import urllib.request
from pathlib import Path

from ingest.fixtures import path_for
from ingest.sources.gold import GOLD_PRICES_MONTHLY


def _write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    print(f"  {path.name:<28} {path.stat().st_size:>9,} bytes")


def record_csv(url: str) -> None:
    """Kept whole: the file is small enough that trimming would buy nothing."""
    with urllib.request.urlopen(url, timeout=60) as response:
        _write(path_for(url), response.read())


def main() -> None:
    record_csv(GOLD_PRICES_MONTHLY)


if __name__ == "__main__":
    main()
