#!/usr/bin/env python3
"""
Read a CSV file and send each row as a JSON body via PUT to an API endpoint.

Each row produces a payload like:

{
  "resources": [
    {
      "<col1>": <value1>,
      "<col2>": <value2>,
      ...
    }
  ],
  "resourceId": "<from --resource-id or CSV column>"
}

For your training example, with appropriate CSV headers, that becomes:

{
  "resources": [
    {
      "status": "COMPLETE",
      "displayName": "CustomTrainingOne",
      "uniqueId": "customtraining_001",
      "externalUrl": "https://trainingplatform.com/customtraining_001",
      "trainingId": "custom-training-1",
      "trainingName": "Custom Training One",
      "frameworksFulfilled": ["SOC2", "ISO27001"],
      "traineeFullName": "Jefferson Haw",
      "traineeAccountName": "jhaw",
      "traineeEmail": "jefferson.haw@vanta.com",
      "trainingCreatedTimestamp": "2026-02-05T13:30:00",
      "trainingDueTimestamp": "2026-03-05T23:59:59",
      "trainingCompletedTimestamp": "2026-02-24T15:30:00"
    }
  ],
  "resourceId": "69a8de32dc99c4b3fa748168"
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


def send_row_put(
    url: str,
    payload: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int = 10,
) -> requests.Response:
    """Send a single payload as JSON via PUT."""
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

    # If already RFC3339 with Z, keep it
    try:
        # This will succeed on 'YYYY-MM-DDTHH:MM:SSZ'
        if v.endswith("Z"):
            datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ")
            return v
    except ValueError:
        pass

    formats = [
        "%m/%d/%Y %H:%M:%S",   # 02/05/2026 13:30:00  (MM/DD/YYYY)
        "%d/%m/%Y %H:%M:%S",   # 05/02/2026 13:30:00  (DD/MM/YYYY)
        "%Y-%m-%d %H:%M:%S",   # 2026-02-05 13:30:00
        "%Y-%m-%dT%H:%M:%S",   # 2026-02-05T13:30:00
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(v, fmt)
            # Treat as UTC and add Z
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue

    # If nothing matched, just return the original
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

    # Booleans
    if v.lower() == "true":
        return True
    if v.lower() == "false":
        return False

    # Try JSON (for arrays/objects, e.g. ["SOC2","ISO27001"])
    if (v.startswith("[") and v.endswith("]")) or (v.startswith("{") and v.endswith("}")):
        try:
            return json.loads(v)
        except json.JSONDecodeError:
            pass

    return v


def build_payload(
    csv_row: Dict[str, Any],
    headers: List[str],
    resource_id_override: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build the JSON body to send for each request.

    Final format:

        {
          "resources": [
            {
              "<header1>": <value1>,
              "<header2>": <value2>,
              ...
            }
          ],
          "resourceId": "<id>"
        }

    - resourceId: from override or CSV column 'resourceId'
    - all other headers become fields on resources[0]
    - trainingCreatedTimestamp / trainingDueTimestamp / trainingCompletedTimestamp
      are parsed & normalized to ISO-8601 where possible.
    """
    # Decide resourceId
    resource_id = resource_id_override or csv_row.get("resourceId")
    if not resource_id:
        raise ValueError(
            "resourceId is missing: provide --resource-id or a 'resourceId' column in the CSV."
        )

    resource_obj: Dict[str, Any] = {}
    for h in headers:
        if h == "resourceId":
            continue  # used at top level only

        raw_value = csv_row.get(h)
        if raw_value in (None, ""):
            continue

      # Special handling for timestamp fields
        if h in TIMESTAMP_FIELDS:
            resource_obj[h] = normalize_timestamp(raw_value)
        else:
            resource_obj[h] = coerce_value(raw_value)

    return {
        "resources": [resource_obj],
        "resourceId": resource_id,
    }


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

            # Build final payload in your desired format
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