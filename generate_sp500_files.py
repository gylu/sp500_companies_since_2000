#!/usr/bin/env python3
"""
Generate monthly S&P 500 constituent files from January 2000 to today.

Files are named spy500_yyyy_mm_01.txt and contain one ticker symbol per line,
representing the S&P 500 composition on the first of each month.

Data is sourced from:
  https://en.wikipedia.org/wiki/List_of_S%26P_500_companies

Usage:
    python generate_sp500_files.py              # create only missing files
    python generate_sp500_files.py --repopulate # recreate all files
"""

import argparse
import os
import re
import sys
from datetime import date, datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup

WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
START_DATE = date(2000, 1, 1)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; sp500-history-bot/1.0; "
        "+https://github.com/gylu/sp500_companies_since_2000)"
    )
}


def fetch_soup(url: str) -> BeautifulSoup:
    """Fetch a URL and return a parsed BeautifulSoup object."""
    response = requests.get(url, headers=_HEADERS, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.content, "lxml")


def _clean_ticker(text: str) -> str:
    """Strip whitespace and footnote markers (e.g. [1]) from a ticker string."""
    text = text.strip()
    text = re.sub(r"\[.*?\]", "", text).strip()
    return text


def fetch_current_sp500(soup: BeautifulSoup) -> set[str]:
    """
    Parse the 'constituents' table from the Wikipedia page and return the set
    of current S&P 500 ticker symbols.
    """
    table = soup.find("table", {"id": "constituents"})
    if table is None:
        raise ValueError(
            "Could not find the constituents table (id='constituents') on the Wikipedia page."
        )

    tickers = set()
    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if cols:
            ticker = _clean_ticker(cols[0].get_text())
            if ticker:
                tickers.add(ticker)
    return tickers


def _parse_change_date(text: str) -> Optional[date]:
    """Try several date formats used in the Wikipedia changes table."""
    text = text.strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def fetch_changes(soup: BeautifulSoup) -> list[tuple[date, str, str]]:
    """
    Parse the 'changes' table from the Wikipedia page.

    Returns a list of (change_date, added_ticker, removed_ticker) tuples
    sorted in descending (most-recent-first) order.  Either added_ticker or
    removed_ticker may be an empty string when the event is one-sided.
    """
    table = soup.find("table", {"id": "changes"})
    if table is None:
        raise ValueError(
            "Could not find the changes table (id='changes') on the Wikipedia page."
        )

    changes = []
    for row in table.find_all("tr"):
        cols = row.find_all("td")
        if not cols:
            # header row – skip
            continue

        change_date = _parse_change_date(cols[0].get_text())
        if change_date is None:
            continue

        # cols[1] = added ticker, cols[3] = removed ticker
        added = _clean_ticker(cols[1].get_text()) if len(cols) > 1 else ""
        removed = _clean_ticker(cols[3].get_text()) if len(cols) > 3 else ""

        # Skip rows where both fields are blank (malformed / continuation rows)
        if not added and not removed:
            continue

        changes.append((change_date, added, removed))

    changes.sort(key=lambda x: x[0], reverse=True)
    return changes


def generate_month_starts(start: date, end: date) -> list[date]:
    """Return a list of first-of-month dates from *start* through *end* (inclusive)."""
    months = []
    current = date(start.year, start.month, 1)
    end_first = date(end.year, end.month, 1)
    while current <= end_first:
        months.append(current)
        year, month = current.year, current.month
        if month == 12:
            current = date(year + 1, 1, 1)
        else:
            current = date(year, month + 1, 1)
    return months


def get_filepath(d: date) -> str:
    """Return the full path for the file corresponding to *d*."""
    filename = f"spy500_{d.year:04d}_{d.month:02d}_01.txt"
    return os.path.join(OUTPUT_DIR, filename)


def write_snapshot(filepath: str, tickers: set[str]) -> None:
    """Write sorted ticker symbols to *filepath*, one per line."""
    with open(filepath, "w", encoding="utf-8") as fh:
        for ticker in sorted(tickers):
            fh.write(ticker + "\n")


def build_snapshots(
    current_sp500: set[str],
    changes: list[tuple[date, str, str]],
    all_months: list[date],
) -> dict[date, set[str]]:
    """
    Build a mapping of {first-of-month date: set of tickers} by starting from
    the current S&P 500 composition and walking backwards in time, undoing each
    change as we pass its date.

    A change record (change_date, added, removed) means:
      - *added* was added to the index on *change_date*
      - *removed* was removed from the index on *change_date*

    To reconstruct the composition on a date D *before* change_date we:
      - remove *added* (undo the addition)
      - add back *removed* (undo the removal)

    The composition on a month M includes all changes whose date is <= M (the
    first of the month).  Therefore we undo changes whose date is strictly
    greater than M.
    """
    composition = set(current_sp500)
    snapshots = {}

    # Both lists are in descending order; changes pointer tracks position
    change_idx = 0
    total_changes = len(changes)

    for month in sorted(all_months, reverse=True):
        # Undo all changes that happened strictly after this month's start date
        while change_idx < total_changes and changes[change_idx][0] > month:
            _, added, removed = changes[change_idx]
            if added:
                composition.discard(added)   # undo the addition
            if removed:
                composition.add(removed)     # undo the removal
            change_idx += 1

        snapshots[month] = set(composition)

    return snapshots


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Generate monthly S&P 500 constituent files (spy500_yyyy_mm_01.txt) "
            "from January 2000 to today, based on Wikipedia data."
        )
    )
    parser.add_argument(
        "--repopulate",
        action="store_true",
        default=False,
        help="Recreate all files, even those that already exist (default: False).",
    )
    args = parser.parse_args(argv)

    today = date.today()
    all_months = generate_month_starts(START_DATE, today)

    # Determine which months still need a file
    if args.repopulate:
        months_needed = set(all_months)
    else:
        months_needed = {m for m in all_months if not os.path.exists(get_filepath(m))}

    if not months_needed:
        print(
            "All files are already present. "
            "Run with --repopulate to recreate them."
        )
        return 0

    print(f"Fetching S&P 500 data from Wikipedia...")
    try:
        soup = fetch_soup(WIKIPEDIA_URL)
    except requests.RequestException as exc:
        print(f"ERROR: Could not fetch Wikipedia page: {exc}", file=sys.stderr)
        return 1

    current_sp500 = fetch_current_sp500(soup)
    print(f"  Current S&P 500: {len(current_sp500)} companies")

    changes = fetch_changes(soup)
    print(f"  Historical change records: {len(changes)}")

    snapshots = build_snapshots(current_sp500, changes, all_months)

    created = 0
    for month in all_months:
        if month not in months_needed:
            continue
        snapshot = snapshots.get(month)
        if snapshot is None:
            print(f"  WARNING: No snapshot computed for {month}", file=sys.stderr)
            continue
        write_snapshot(get_filepath(month), snapshot)
        created += 1

    print(f"Done. Created {created} file(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
