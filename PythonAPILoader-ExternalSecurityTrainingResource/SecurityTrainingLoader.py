#!/usr/bin/env python3
"""
Read a CSV file and send all rows as a single JSON body via PUT to an API endpoint.

Final payload shape:

{
  "resources": [
    {
      "<col1>": <value1>,
      "<col2>": <value2>,
      ...
    },
    {
      "<col1>": <value1>,
      "<col2>": <value2>,
      ...
    }
  ],
  "resourceId": "<from --resource-id or CSV column>"
}
"""

import csv
import json
import argparse
import requests
from typing import Dict, Any, List, Optional
from datetime import datetime

# Fields that should be treated as timestamps
TIMESTAMP_FIELDS = {
    "trainingCreatedTimestamp",
    "trainingDueTimestamp",
    "trainingCompletedTimestamp",
}


def send_bulk_put(
    url: str,
    payload: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int = 10,
) -> requests.Response:
    """Send one bulk payload as JSON via PUT."""
    response = requests.put(url, json=payload, headers=headers, timeout=timeout)
    return response


def normalize_timestamp(value: str) -> str:
    """
    Try to parse a timestamp string and normalize it to RFC3339/ISO-8601
    with a trailing 'Z', e.g. '2026-02-05T13:30:00Z'.

    Supported input formats (examples):
      - "02/05/2026 13:30:00"  (MM/DD/YYYY HH:MM:SS)
      - "05/02/2026 13:30:00"  (DD/MM/YYYY HH:MM:SS)
      - "2026-02-05 13:30:00"
      - "2026-02-05T13:30:00"
      - "2026-02-05T13:30:00Z"

    If parsing fails, returns the original string unchanged.
    """
    v = value.strip()

    try:
        if v.endswith("Z"):
            datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ")
            return v
    except ValueError:
        pass

    formats = [
        "%m/%d/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(v, fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue

    return value


def coerce_value(value: Any) -> Any:
    """
    Try to convert CSV string values into more appropriate JSON types.

    - "true"/"false" (case-insensitive) -> bool
    - Strings that look like JSON arrays/objects -> parsed via json.loads
    - Everything else stays as a string
    """
    if not isinstance(value, str):
        return value

    v = value.strip()

    if v.lower() == "true":
        return True
    if v.lower() == "false":
        return False

    if (v.startswith("[") and v.endswith("]")) or (v.startswith("{") and v.endswith("}")):
        try:
            return json.loads(v)
        except json.JSONDecodeError:
            pass

    return v


def build_resource_object(
    csv_row: Dict[str, Any],
    headers: List[str],
) -> Dict[str, Any]:
    """Build one resource object from a CSV row."""
    resource_obj: Dict[str, Any] = {}

    for h in headers:
        if h == "resourceId":
            continue

        raw_value = csv_row.get(h)
        if raw_value in (None, ""):
            continue

        if h in TIMESTAMP_FIELDS:
            resource_obj[h] = normalize_timestamp(raw_value)
        else:
            resource_obj[h] = coerce_value(raw_value)

    return resource_obj


def build_bulk_payload(
    csv_rows: List[Dict[str, Any]],
    headers: List[str],
    resource_id_override: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build one bulk JSON body for all CSV rows.

    - resourceId comes from --resource-id or the CSV resourceId column
    - if resourceId is taken from CSV, all rows must have the same value
    """
    if not csv_rows:
        raise ValueError("CSV file contains no data rows.")

    if resource_id_override:
        resource_id = resource_id_override
    else:
        resource_ids = []
        for idx, row in enumerate(csv_rows, start=1):
            row_resource_id = row.get("resourceId")
            if not row_resource_id:
                raise ValueError(
                    f"resourceId is missing in row #{idx}: provide --resource-id or a 'resourceId' column in the CSV."
                )
            resource_ids.append(str(row_resource_id))

        unique_resource_ids = sorted(set(resource_ids))
        if len(unique_resource_ids) != 1:
            raise ValueError(
                "All rows must have the same resourceId for a single bulk request. "
                f"Found: {unique_resource_ids}"
            )
        resource_id = unique_resource_ids[0]

    resources = [build_resource_object(row, headers) for row in csv_rows]

    return {
        "resources": resources,
        "resourceId": resource_id,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Read a CSV file and send all rows in one bulk PUT request to an API endpoint."
    )
    parser.add_argument("csv_path", help="Path to the input CSV file")
    parser.add_argument("api_url", help="API endpoint URL to send the PUT request to")

    parser.add_argument(
        "--auth-token",
        help="Optional Bearer auth token (e.g. for Authorization: Bearer <token>)",
    )
    parser.add_argument(
        "--id-column",
        help=(
            "Optional: column name to append to the URL as /<value> "
            "(e.g. api_url/<id>). For bulk mode, all rows must share the same value."
        ),
    )
    parser.add_argument(
        "--resource-id",
        help=(
            "Optional: static resourceId to use in the payload. "
            "If not provided, the script will look for a 'resourceId' column "
            "in the CSV, and all rows must have the same value."
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
        help="Print what would be sent instead of making the request",
    )

    args = parser.parse_args()

    headers = {"Content-Type": "application/json"}
    if args.auth_token:
        headers["Authorization"] = f"Bearer {args.auth_token}"

    with open(args.csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        if reader.fieldnames is None:
            raise ValueError("CSV file appears to have no header row / fieldnames.")

        csv_headers = [h.strip() for h in reader.fieldnames if h is not None]
        print(f"Fields detected in CSV: {csv_headers}")

        cleaned_rows: List[Dict[str, Any]] = []
        for row in reader:
            cleaned_row = {
                (k.strip() if isinstance(k, str) else k): (
                    v.strip() if isinstance(v, str) else v
                )
                for k, v in row.items()
            }
            cleaned_rows.append(cleaned_row)

    url = args.api_url
    if args.id_column:
        id_values = []
        for row in cleaned_rows:
            if args.id_column not in row:
                raise KeyError(
                    f"Configured id-column '{args.id_column}' not found in CSV columns: {csv_headers}"
                )
            id_value = row.get(args.id_column)
            if not id_value:
                raise ValueError(
                    f"Configured id-column '{args.id_column}' is empty in one or more rows."
                )
            id_values.append(str(id_value))

        unique_id_values = sorted(set(id_values))
        if len(unique_id_values) != 1:
            raise ValueError(
                "All rows must have the same id-column value for a single bulk request. "
                f"Found: {unique_id_values}"
            )

        url = f"{args.api_url.rstrip('/')}/{unique_id_values[0]}"

    payload = build_bulk_payload(
        cleaned_rows,
        csv_headers,
        resource_id_override=args.resource_id,
    )

    if args.dry_run:
        print("\n[DRY RUN] Bulk request")
        print(f"PUT {url}")
        print("Payload:")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return

    try:
        resp = send_bulk_put(url, payload, headers, timeout=args.timeout)
    except requests.RequestException as e:
        print(f"[ERROR] Bulk request failed: {e}")
        return

    if 200 <= resp.status_code < 300:
        print(
            f"[OK] Bulk request sent {len(payload['resources'])} resources to {url} "
            f"(status {resp.status_code})"
        )
    else:
        print(
            f"[FAIL] Bulk request -> {url} (status {resp.status_code}) "
            f"Response: {resp.text[:500]}"
        )


if __name__ == "__main__":
    main()
