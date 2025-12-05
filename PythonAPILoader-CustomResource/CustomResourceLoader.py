#!/usr/bin/env python3
"""
Send CSV rows as JSON via API PUT requests.

Each row produces a payload like:

{
  "resourceId": "<from --resource-id or CSV column>",
  "resources": [
    {
      "displayName": "<from CSV displayName>",
      "uniqueId": "<from CSV uniqueId>",
      "externalUrl": "<from CSV externalUrl>",
      "customProperties": {
        "<otherColumn1>": "<value1>",
        "<otherColumn2>": "<value2>"
      }
    }
  ]
}
"""

import csv
import json
import argparse
import requests
from typing import Dict, Any, List, Optional


def send_row_put(
    url: str,
    payload: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int = 10,
) -> requests.Response:
    """Send a single payload as JSON via PUT."""
    response = requests.put(url, json=payload, headers=headers, timeout=timeout)
    return response


def build_payload(
    csv_row: Dict[str, Any],
    csv_headers: List[str],
    resource_id_override: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build the JSON body to send for each request.

    Mapping rules:
      - resourceId:
          - if resource_id_override is provided, use that
          - otherwise, use csv_row["resourceId"] (if present)
      - displayName, uniqueId, externalUrl:
          - from CSV columns with those names (if present)
      - customProperties:
          - all other columns (excluding resourceId, displayName, uniqueId, externalUrl)
    """

    # Determine resourceId
    resource_id = resource_id_override or csv_row.get("resourceId")
    if resource_id is None:
        raise ValueError(
            "resourceId is missing: provide --resource-id or a 'resourceId' column in the CSV."
        )

    # Pull standard fields if present
    display_name = csv_row.get("displayName")
    unique_id = csv_row.get("uniqueId")
    external_url = csv_row.get("externalUrl")

    # Build customProperties from remaining columns
    excluded_keys = {"resourceId", "displayName", "uniqueId", "externalUrl"}
    custom_properties = {
        k: v
        for k, v in csv_row.items()
        if k not in excluded_keys and v not in (None, "")
    }

    resource_obj: Dict[str, Any] = {
        "displayName": display_name,
        "uniqueId": unique_id,
        "externalUrl": external_url,
        "customProperties": custom_properties,
    }

    # Remove keys that are None, if you want to avoid sending nulls
    resource_obj = {k: v for k, v in resource_obj.items() if v is not None}

    payload = {
        "resourceId": resource_id,
        "resources": [resource_obj],
    }

    return payload


def main():
    parser = argparse.ArgumentParser(
        description="Read a CSV file and send each row via PUT to an API endpoint."
    )
    parser.add_argument("csv_path", help="Path to the input CSV file")
    parser.add_argument("api_url", help="API endpoint URL to send PUT requests to")

    parser.add_argument(
        "--auth-token",
        help="Optional Bearer auth token (e.g. for Authorization: Bearer <token>)",
    )
    parser.add_argument(
        "--id-column",
        help=(
            "Optional: column name to append to the URL as /<value> "
            "(e.g. api_url/<id>)"
        ),
    )
    parser.add_argument(
        "--resource-id",
        help=(
            "Optional: static resourceId to use in the payload. "
            "If not provided, the script will look for a 'resourceId' column "
            "in the CSV."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Request timeout in seconds (default: 10)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be sent instead of making requests",
    )

    args = parser.parse_args()

    # Base headers
    headers = {"Content-Type": "application/json"}
    if args.auth_token:
        headers["Authorization"] = f"Bearer {args.auth_token}"

    # Open and read CSV
    with open(args.csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        if reader.fieldnames is None:
            raise ValueError("CSV file appears to have no header row / fieldnames.")

        csv_headers = [h.strip() for h in reader.fieldnames if h is not None]
        print(f"Fields detected in CSV: {csv_headers}")

        for i, row in enumerate(reader, start=1):
            # Build URL (optionally with /<id> at the end)
            url = args.api_url
            if args.id_column:
                if args.id_column not in row:
                    raise KeyError(
                        f"Configured id-column '{args.id_column}' not found in CSV "
                        f"columns: {csv_headers}"
                    )
                url = f"{args.api_url.rstrip('/')}/{row[args.id_column]}"

            # Clean CSV row: strip whitespace from keys & values
            cleaned_row = {
                (k.strip() if isinstance(k, str) else k): (
                    v.strip() if isinstance(v, str) else v
                )
                for k, v in row.items()
            }

            # Build final payload matching your JSON format
            payload = build_payload(
                cleaned_row,
                csv_headers,
                resource_id_override=args.resource_id,
            )

            if args.dry_run:
                print(f"\n[DRY RUN] Row #{i}")
                print(f"PUT {url}")
                print("Payload:")
                print(json.dumps(payload, indent=2, ensure_ascii=False))
                continue

            try:
                resp = send_row_put(url, payload, headers, timeout=args.timeout)
            except requests.RequestException as e:
                print(f"[ERROR] Row #{i}: request failed: {e}")
                continue

            if 200 <= resp.status_code < 300:
                print(f"[OK] Row #{i} -> {url} (status {resp.status_code})")
            else:
                print(
                    f"[FAIL] Row #{i} -> {url} (status {resp.status_code}) "
                    f"Response: {resp.text[:500]}"
                )


if __name__ == "__main__":
    main()
