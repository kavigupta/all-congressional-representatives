# All U.S. House Representatives

This repository contains a script and exported data for U.S. House representatives across Congresses.

## Fields

The main output file, `representatives.csv`, includes:

- `representative_name`: Representative display name as listed in the source page, or `Vacant` if the seat was vacant.
- `representative_wikipedia_page`: Full Wikipedia URL for the representative, or empty if the seat was vacant or the representative does not have a Wikipedia page.
- `term`: Congress number for the row (for example, `119`).
- `state`: State or territory name associated with the seat.
- `district`: District identifier (numeric district or `AL` for at-large).
- `vacant`: `True` if the seat was vacant for that row, otherwise `False`.
- `party`: Normalized party name for the representative at that time.

The `party_pages.json` file maps party names to Wikipedia page URLs when available.

## Notes

- This project was written with AI assistance, so there may be errors.
- The data is only as up to date as the most recent timestamp on `representatives.csv`.
