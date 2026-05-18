# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#
"""
Standalone script to fetch MemberLink data from the ECB BIRD /query endpoint.

The /excel/tree and /excel/export/metadata endpoints are unreliable (frequent
HTTP 500s). This script uses the more stable /query endpoint, which returns
paginated JSON, to download all MemberLink rows and write them as a CSV
matching the format the derivation pipeline expects.

Usage:
    cd birds_nest && uv run pybirdai/standalone/fetch_member_link_via_query.py
"""
import csv
import json
import os
import sys
import logging

import requests

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

QUERY_URL = "https://bird.ecb.europa.eu/query"

HEADERS = {
    "Content-Type": "application/json",
    "x-sdd-app": "true",
    "x-sdd-correlation-description": "Query Content Management MemberLink Table Data",
    "Referer": "https://bird.ecb.europa.eu/cm",
}

PAGE_SIZE = 5000
MAX_PAGES = 200  # safety cap: 1M rows

# JSON camelCase key -> CSV UPPER_SNAKE column
FIELD_MAP = {
    "cubeStructureItemLinkId": "CUBE_STRUCTURE_ITEM_LINK_ID",
    "foreignMemberId": "FOREIGN_MEMBER_ID",
    "primaryMemberId": "PRIMARY_MEMBER_ID",
    "validFrom": "VALID_FROM",
    "validTo": "VALID_TO",
    "isLinked": "IS_LINKED",
}

CSV_COLUMNS = list(FIELD_MAP.values())

OUTPUT_DIR = "resources/derivation_files"
OUTPUT_FILENAME = "member_link_for_derivation.csv"


def build_payload(offset: int) -> dict:
    return {
        "type": "MemberLink",
        "filter": {
            "sort": [],
            "type": "MemberLink",
            "bid": {"in": [1]},
        },
        "limit": {
            "limit": PAGE_SIZE,
            "offset": offset,
        },
    }


def normalize_is_linked(value) -> str:
    """Normalize isLinked to lowercase 'true'/'false'."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value.lower()
    return str(value).lower()


def map_row(raw: dict) -> dict:
    """Map a single JSON object to a CSV row dict."""
    row = {}
    for json_key, csv_col in FIELD_MAP.items():
        value = raw.get(json_key, "")
        if csv_col == "IS_LINKED":
            value = normalize_is_linked(value)
        row[csv_col] = value if value is not None else ""
    return row


def fetch_all_rows() -> list[dict]:
    """Paginate through the /query endpoint and collect all rows."""
    all_rows = []
    offset = 0

    for page_num in range(1, MAX_PAGES + 1):
        payload = build_payload(offset)
        logger.info("Fetching page %d (offset=%d) ...", page_num, offset)

        response = requests.post(
            QUERY_URL, headers=HEADERS, json=payload, timeout=120
        )

        if response.status_code != 200:
            logger.error(
                "HTTP %d from %s\nResponse body: %s",
                response.status_code,
                QUERY_URL,
                response.text[:2000],
            )
            sys.exit(1)

        data = response.json()

        # The response may be a list directly or wrapped in an object.
        # Log the structure on the first page so we can confirm.
        if page_num == 1:
            if isinstance(data, dict):
                logger.info("Response is a dict with keys: %s", list(data.keys()))
                # Try common wrapper keys
                rows = data.get("content") or data.get("data") or data.get("results") or data.get("rows") or []
                if not rows and isinstance(data, dict):
                    # Maybe the dict itself contains row-like data
                    logger.warning(
                        "Could not find rows in dict. Full first-page response (truncated): %s",
                        json.dumps(data, default=str)[:3000],
                    )
                    sys.exit(1)
            elif isinstance(data, list):
                logger.info("Response is a list with %d items", len(data))
                rows = data
            else:
                logger.error("Unexpected response type: %s", type(data))
                sys.exit(1)

            # Log the keys of the first row for field mapping verification
            if rows:
                logger.info(
                    "First row JSON keys: %s", list(rows[0].keys()) if isinstance(rows[0], dict) else "N/A"
                )
                logger.info("First row sample: %s", json.dumps(rows[0], default=str)[:1000])
        else:
            # Subsequent pages: extract rows the same way
            if isinstance(data, dict):
                rows = data.get("content") or data.get("data") or data.get("results") or data.get("rows") or []
            elif isinstance(data, list):
                rows = data
            else:
                rows = []

        page_count = len(rows)
        all_rows.extend(rows)
        logger.info("Page %d returned %d rows (total so far: %d)", page_num, page_count, len(all_rows))

        if page_count < PAGE_SIZE:
            # Last page
            break

        offset += PAGE_SIZE

    return all_rows


def write_csv(rows: list[dict], output_path: str) -> None:
    """Write mapped rows to CSV."""
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for raw in rows:
            writer.writerow(map_row(raw))


def main():
    logger.info("="*80)
    logger.info("FETCH MEMBER_LINK VIA ECB /query ENDPOINT")
    logger.info("="*80)

    raw_rows = fetch_all_rows()

    if not raw_rows:
        logger.warning("No rows returned from the API.")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

    write_csv(raw_rows, output_path)

    logger.info("="*80)
    logger.info("Done. Wrote %d rows to %s", len(raw_rows), output_path)
    logger.info("="*80)


if __name__ == "__main__":
    main()
