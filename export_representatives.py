#!/usr/bin/env python3
"""DISCLAIMER: This parser intentionally does not validate the third Ushr argument.

Export current U.S. representatives from Wikipedia to CSV.

The script uses the MediaWiki API to fetch the wikitext for the Wikipedia page
"List of current United States representatives", parses the "List of
representatives" table, and writes a CSV with:

- representative name
- representative Wikipedia page URL
- term (assumed office)
- state
- district

No third-party dependencies are required.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
from dataclasses import asdict, dataclass
import json
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

import us


API_URL = "https://en.wikipedia.org/w/api.php"
DEFAULT_PAGE_TITLE = "List of current United States representatives"
REPRESENTATIVE_CELL_INDEX = 1
DISTRICT_CELL_INDEX = 0
TERM_CELL_INDEX = 7
CSV_FIELDS = [
    "representative_name",
    "representative_wikipedia_page",
    "term",
    "state",
    "district",
]
MIN_MODERN_CONGRESS = 64
MIN_MODERN_REPRESENTATIVE_ROWS = 430
SPECIAL_STATE_IDENTIFIERS = {
    "American Samoa": "American Samoa",
    "Arizona Territory": "Arizona Territory",
    "Dakota Territory": "Dakota Territory",
    "District of Columbia": "District of Columbia",
    "Illinois Territory": "Illinois Territory",
    "Indiana Territory": "Indiana Territory",
    "Guam": "Guam",
    "New Mexico Territory": "New Mexico Territory",
    "Northern Mariana Islands": "Northern Mariana Islands",
    "Idaho Territory": "Idaho Territory",
    "Mississippi Territory": "Mississippi Territory",
    "Oklahoma Territory": "Oklahoma Territory",
    "Puerto Rico": "Puerto Rico",
    "United States Virgin Islands": "United States Virgin Islands",
    "Missouri Territory": "Missouri Territory",
    "Montana Territory": "Montana Territory",
    "Utah Territory": "Utah Territory",
    "Washington Territory": "Washington Territory",
    "Wyoming Territory": "Wyoming Territory",
}


@dataclass(frozen=True)
class RepresentativeRow:
    representative_name: str
    representative_wikipedia_page: str
    term: str
    state: str
    district: str


class AssumptionViolationError(RuntimeError):
    """Raised when page structure or data violates parser assumptions."""


def resolve_state_name(identifier: str, context: str) -> str:
    cleaned = identifier.strip()
    state = us.states.lookup(cleaned)
    if state is not None:
        return state.name

    special = SPECIAL_STATE_IDENTIFIERS.get(cleaned)
    if special is not None:
        return special

    raise AssumptionViolationError(
        f"Unknown state identifier {cleaned!r} in {context}."
    )


def fetch_wikitext(page_title: str) -> str:
    params = urllib.parse.urlencode(
        {
            "action": "parse",
            "page": page_title,
            "prop": "wikitext",
            "format": "json",
            "formatversion": "2",
        }
    )
    url = f"{API_URL}?{params}"
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; WikipediaCSVExport/1.0; +https://example.com)",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)

    try:
        return payload["parse"]["wikitext"]
    except KeyError as exc:  # pragma: no cover - defensive API error handling
        raise AssumptionViolationError(
            f"Wikipedia API response missing parse.wikitext for page {page_title!r}. Payload keys: {list(payload.keys())}"
        ) from exc


def extract_congress_number_from_current_page(wikitext: str) -> int:
    match = re.search(r"\[\[(\d+)(?:st|nd|rd|th) United States Congress\|", wikitext)
    if not match:
        raise AssumptionViolationError(
            "Could not determine current Congress number from current representatives page wikitext."
        )
    return int(match.group(1))


def congress_page_title(congress_number: int) -> str:
    suffix = "th"
    if congress_number % 100 not in (11, 12, 13):
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(congress_number % 10, "th")
    return f"{congress_number}{suffix} United States Congress"


def extract_infobox_field(wikitext: str, field_name: str) -> str:
    pattern = re.compile(rf"^\|\s*{re.escape(field_name)}\s*=\s*(.+)$", re.MULTILINE)
    match = pattern.search(wikitext)
    if not match:
        raise AssumptionViolationError(f"Missing required infobox field {field_name!r}.")
    value = match.group(1).strip()
    value = re.sub(r"<!--.*?-->", "", value).strip()
    return value


def format_simple_date(date_text: str) -> str:
    cleaned = re.sub(r"''([^']+)''", r"\1", date_text).strip()
    try:
        date = dt.datetime.strptime(cleaned, "%B %d, %Y").date()
    except ValueError:
        raise AssumptionViolationError(
            f"Expected date in 'Month D, YYYY' format but found {cleaned!r}."
        )
    return f"{date.strftime('%B')} {date.day}, {date.year}"


def congress_term_start(wikitext: str) -> str:
    start_text = extract_infobox_field(wikitext, "start")
    return format_simple_date(start_text)


def extract_representatives_table(wikitext: str) -> str:
    start_marker = "==List of representatives=="
    end_marker = "==List of delegates=="

    start = wikitext.find(start_marker)
    end = wikitext.find(end_marker)
    if start == -1 or end == -1 or end <= start:
        raise AssumptionViolationError(
            "Could not locate expected 'List of representatives' table boundaries in current page wikitext."
        )

    return wikitext[start:end]


def extract_congress_representatives_section(wikitext: str) -> str:
    end_patterns = [
        r"^==\s*Changes in membership\s*==$",
        r"^====\s*Non-voting members\s*====$",
        r"^==\s*Committees\s*==$",
    ]

    start_match = re.search(r"^===\s*(?:Representatives|House of Representatives)\s*===$", wikitext, re.MULTILINE)
    if not start_match:
        raise AssumptionViolationError(
            "Could not locate expected Representatives heading in Congress page wikitext."
        )
    start = start_match.start()

    end_candidates = []
    for pattern in end_patterns:
        match = re.search(pattern, wikitext[start:], re.MULTILINE)
        if match:
            end_candidates.append(start + match.start())
    if not end_candidates:
        raise AssumptionViolationError(
            "Could not locate end marker for Representatives section in Congress page wikitext."
        )

    end = min(end_candidates)
    return wikitext[start:end]


def parse_congress_representatives(page_wikitext: str, term: str) -> list[RepresentativeRow]:
    section = extract_congress_representatives_section(page_wikitext)
    rows: list[RepresentativeRow] = []

    entry_pattern = re.compile(
        r"\{\{[Uu]shr\|(?P<state>[^|}]+)\|(?P<district>[^|}]+)\|(?P<mode>[^|}]+)\}\}(?:\.|)\s*"
        r"(?:\{\{Party stripe\|[^}]+\}\})?"
        r"\[\[(?P<target>[^\[\]]+)\]\]",
        re.DOTALL,
    )

    for match in entry_pattern.finditer(section):
        state_text = match.group("state").strip()
        district_text = match.group("district").strip()
        target = match.group("target").strip()
        if target.lower() == "vacant":
            raise AssumptionViolationError(
                f"Encountered vacant seat in historical parser for state {state_text!r}, district {district_text!r}."
            )

        page_title = target.split("|", 1)[0].strip()
        display_name = target.split("|", 1)[-1].strip() if "|" in target else page_title
        state_name = resolve_state_name(state_text, "Congress representatives section")

        rows.append(
            RepresentativeRow(
                representative_name=display_name,
                representative_wikipedia_page=to_wikipedia_url(page_title),
                term=term,
                state=state_name,
                district=district_text,
            )
        )

    if not rows:
        ushr_count = section.count("{{ushr|") + section.count("{{Ushr|")
        elected_marker_count = section.count("|E}}")
        numeric_mode_count = len(re.findall(r"\{\{[Uu]shr\|[^|}]+\|[^|}]+\|\d+\}\}", section))
        at_large_mode_count = len(re.findall(r"\{\{[Uu]shr\|[^|}]+\|[^|}]+\|At-large\}\}", section))
        raise AssumptionViolationError(
            "Representatives section found, but historical entry_pattern matched zero rows. "
            "Observed tokens: "
            f"'{{{{ushr|'={ushr_count}, '|E}}}}'={elected_marker_count}, "
            f"'numeric_mode'={numeric_mode_count}, 'At-large_mode'={at_large_mode_count}. "
            "Assumption violated: expected Ushr templates in parseable ': {{Party stripe...}}{{Ushr...}}. [[Member]]' entry format."
        )

    return rows


def split_rows(table_wikitext: str) -> list[str]:
    rows: list[str] = []
    chunks = re.split(r"(?m)^\|-(?:.*)$", table_wikitext)
    for index, chunk in enumerate(chunks):
        if index == 0:
            continue

        block = chunk.strip()
        if not block:
            continue
        if block.startswith(("|}", "|+", "<!--", "!scope=\"col\"")):
            continue

        rows.append(block)

    return rows


def split_cells(row_block: str) -> list[str]:
    cells: list[str] = []
    current: list[str] = []

    def clean_line(line: str) -> str:
        if line.startswith('!scope="row" |'):
            return line.split("|", 1)[1].strip()
        if line.startswith("|") and " |" in line[1:]:
            return line.split(" |", 1)[1].strip()
        if line.startswith("|"):
            return line[1:].strip()
        return line.strip()

    for raw_line in row_block.splitlines():
        line = raw_line.rstrip()
        if not line:
            continue
        if line.startswith("!scope=\"row\"") or line.startswith("|"):
            if current:
                cells.append("\n".join(current).strip())
            current = [clean_line(line)]
        else:
            if current:
                current.append(clean_line(line))

    if current:
        cells.append("\n".join(current).strip())

    return cells


def extract_state_and_district(district_cell: str) -> tuple[str, str]:
    district_markup = district_cell.strip()
    match = re.search(r"\{\{ushr\|([A-Z]{2})\|([^|}]+)", district_markup)
    if not match:
        raise AssumptionViolationError(f"Could not parse district cell markup: {district_cell!r}")

    state_abbrev = match.group(1)
    district = match.group(2).strip()
    state_name = resolve_state_name(state_abbrev, f"district cell: {district_cell!r}")
    return state_name, district


def extract_representative(member_cell: str) -> tuple[str, str]:
    member_markup = member_cell.strip()
    for match in re.finditer(r"\[\[([^\[\]]+)\]\]", member_markup):
        target = match.group(1)
        if target.startswith(("File:", "Image:")):
            continue
        page_title = target.split("|", 1)[0].strip()
        display_name = target.split("|", 1)[-1].strip() if "|" in target else page_title
        return display_name, page_title
    raise AssumptionViolationError(f"Could not parse representative link from member cell: {member_cell!r}")


def extract_term(term_cell: str) -> str:
    term_markup = term_cell.strip()
    match = re.search(r"\{\{dts(?:\|format=mdy)?\|(\d{4})\|(\d{1,2})\|(\d{1,2})\}\}", term_markup)
    if not match:
        raise AssumptionViolationError(
            f"Expected dts template in term cell but found: {term_cell!r}"
        )

    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))
    date = dt.date(year, month, day)
    return f"{date.strftime('%B')} {date.day}, {date.year}"


def to_wikipedia_url(page_title: str) -> str:
    slug = page_title.replace(" ", "_")
    return "https://en.wikipedia.org/wiki/" + urllib.parse.quote(slug, safe="()/_")


def parse_rows(table_wikitext: str) -> list[RepresentativeRow]:
    results: list[RepresentativeRow] = []

    for row_index, row_block in enumerate(split_rows(table_wikitext), start=1):
        cells = split_cells(row_block)
        if len(cells) <= TERM_CELL_INDEX:
            raise AssumptionViolationError(
                f"Row {row_index} has {len(cells)} cells; expected at least {TERM_CELL_INDEX + 1}. Row: {row_block!r}"
            )

        state, district = extract_state_and_district(cells[DISTRICT_CELL_INDEX])
        representative_name, page_title = extract_representative(cells[REPRESENTATIVE_CELL_INDEX])
        term = extract_term(cells[TERM_CELL_INDEX])

        results.append(
            RepresentativeRow(
                representative_name=representative_name,
                representative_wikipedia_page=to_wikipedia_url(page_title),
                term=term,
                state=state,
                district=district,
            )
        )

    return results


def parse_historical_rows(start_congress: int, end_congress: int) -> list[RepresentativeRow]:
    rows: list[RepresentativeRow] = []

    for congress_number in range(start_congress, end_congress + 1):
        page_title = congress_page_title(congress_number)
        page_wikitext = fetch_wikitext(page_title)
        term = page_title
        congress_rows = parse_congress_representatives(page_wikitext, term)
        if congress_number >= MIN_MODERN_CONGRESS and len(congress_rows) < MIN_MODERN_REPRESENTATIVE_ROWS:
            raise AssumptionViolationError(
                f"Historical parser produced suspiciously low row count for modern Congress {congress_number}: "
                f"{len(congress_rows)} rows (< {MIN_MODERN_REPRESENTATIVE_ROWS}). "
                "Assumption violated: modern Congress pages should be near full House membership."
            )
        rows.extend(congress_rows)

    return rows


def write_csv(rows: list[RepresentativeRow], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(asdict(row) for row in rows)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--historical",
        action="store_true",
        help="Export representative rows from every United States Congress page up to the current Congress.",
    )
    parser.add_argument(
        "--start-congress",
        type=int,
        help="First Congress number to export when using --historical (default: 1).",
    )
    parser.add_argument(
        "--end-congress",
        type=int,
        help="Last Congress number to export when using --historical (default: current Congress).",
    )
    parser.add_argument(
        "--page-title",
        default=DEFAULT_PAGE_TITLE,
        help=f'Wikipedia page title to read (default: "{DEFAULT_PAGE_TITLE}")',
    )
    parser.add_argument(
        "--output",
        default="representatives.csv",
        help="Output CSV file path (default: representatives.csv)",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    if args.historical:
        current_wikitext = fetch_wikitext(args.page_title)
        current_congress = extract_congress_number_from_current_page(current_wikitext)
        start_congress = args.start_congress if args.start_congress is not None else 1
        end_congress = args.end_congress if args.end_congress is not None else current_congress
        if start_congress < 1 or end_congress < start_congress:
            raise AssumptionViolationError(
                f"Invalid Congress range start={start_congress}, end={end_congress}."
            )
        rows = parse_historical_rows(start_congress, end_congress)
    else:
        wikitext = fetch_wikitext(args.page_title)
        table_wikitext = extract_representatives_table(wikitext)
        rows = parse_rows(table_wikitext)

    if not rows:
        raise AssumptionViolationError("Parser produced zero rows.")

    write_csv(rows, Path(args.output))
    print(f"Wrote {len(rows)} rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))