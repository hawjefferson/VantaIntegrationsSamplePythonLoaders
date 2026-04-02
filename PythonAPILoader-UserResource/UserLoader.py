#!/usr/bin/env python3
"""
Read a CSV file and send all rows as a single bulk JSON body via PUT to an API endpoint.

The request payload looks like:

{
  "resources": [
    {
      "permissionLevel": "BASE",
      "status": "ACTIVE",
      "mfaEnabled": true,
      "mfaMethods": ["PUSH_PROMPT"],
      "authMethod": "SSO",
      "displayName": "John",
      "uniqueId": "Doe",
      "externalUrl": "https://abc.com",
      "fullName": "John Doe",
      "accountName": "jdoe",
      "email": "john.doe@test.com",
      "createdTimestamp": "2025-12-05T02:24:11Z"
    },
    {
      "permissionLevel": "BASE",
      "status": "ACTIVE",
      "mfaEnabled": false,
      "mfaMethods": ["SMS"],
      "authMethod": "SSO",
      "displayName": "Jane",
      "uniqueId": "Smith"
    }
  ],
  "resourceId": "69325a67306d8b286ddc41c1"
}
"""

import csv
import json
import argparse
import requests
from typing import Dict, Any, List, Optional


def send_bulk_put(
    url: str,
    payload: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int = 10,
) -> requests.Response:
    """Send the bulk payload as JSON via PUT."""
    response = requests.put(url, json=payload, headers=headers, timeout=timeout)
    return response


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


def parse_bool(raw_value: Any) -> Optional[bool]:
    """
    Parse a CSV value into a strict boolean for mfaEnabled.

    Accepts (case-insensitive for strings):
      - true values: "true", "1", "yes", "y", "t"
      - false values: "false", "0", "no", "n", "f"
    """
    if raw_value in (None, ""):
        return None

    if isinstance(raw_value, bool):
        return raw_value

    v = str(raw_value).strip().lower()

    if v in ("true", "1", "yes", "y", "t"):
        return True
    if v in ("false", "0", "no", "n", "f"):
        return False

    raise ValueError(f"Cannot parse boolean from value {raw_value!r} for mfaEnabled")


def parse_mfa_methods(raw_value: Any) -> Optional[List[str]]:
    """
    Ensure mfaMethods is always an array.

    Accepts:
      - JSON array string: '["PUSH_PROMPT","SMS"]'
      - Comma-separated string: 'PUSH_PROMPT,SMS'
      - Single value: 'PUSH_PROMPT' -> ['PUSH_PROMPT']
    """
    if raw_value in (None, ""):
        return None

    if not isinstance(raw_value, str):
        if isinstance(raw_value, list):
            return [str(x) for x in raw_value]
        return [str(raw_value)]

    v = raw_value.strip()
    if not v:
        return None

    if v.startswith("[") and v.endswith("]"):
        try:
            parsed = json.loads(v)
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
        except json.JSONDecodeError:
            pass

    if "," in v:
        return [part.strip() for part in v.split(",") if part.strip()]

    return [v]


def build_resource_object(
    csv_row: Dict[str, Any],
    headers: List[str],
) -> Dict[str, Any]:
    """Build a single resource object from one CSV row."""
    resource_obj: Dict[str, Any] = {}

    for h in headers:
        if h == "resourceId":
            continue

        raw_value = csv_row.get(h)
        if raw_value in (None, ""):
            continue

        if h == "mfaMethods":
            parsed = parse_mfa_methods(raw_value)
            if parsed is not None:
                resource_obj[h] = parsed
            continue

        if h == "mfaEnabled":
            parsed_bool = parse_bool(raw_value)
            if parsed_bool is not None:
                resource_obj[h] = parsed_bool
            continue

        resource_obj[h] = coerce_value(raw_value)

    return resource_obj


def build_bulk_payload(
    csv_rows: List[Dict[str, Any]],
    headers: List[str],
    resource_id_override: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build one bulk JSON body for a single request:

    {
      "resources": [{...}, {...}, ...],
      "resourceId": "<from CLI or CSV>"
    }

    Special handling:
      - resourceId: from --resource-id or CSV 'resourceId'
      - mfaMethods: always an array
      - mfaEnabled: always a boolean
    """
    if not csv_rows:
        raise ValueError("CSV file contains no data rows.")

    resource_id = resource_id_override or csv_rows[0].get("resourceId")
    if not resource_id:
        raise ValueError(
            "resourceId is missing: provide --resource-id or a 'resourceId' column in the CSV."
        )

    resources: List[Dict[str, Any]] = []

    for idx, row in enumerate(csv_rows, start=1):
        row_resource_id = row.get("resourceId")
        effective_resource_id = resource_id_override or row_resource_id
        if effective_resource_id != resource_id:
            raise ValueError(
                f"Row #{idx} has resourceId {row_resource_id!r}, which does not match "
                f"the bulk request resourceId {resource_id!r}."
            )

        resources.append(build_resource_object(row, headers))

    return {
        "resources": resources,
        "resourceId": resource_id,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Read a CSV file and send all rows in one bulk PUT request to an API endpoint."
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
            "(e.g. api_url/<id>). For bulk mode, all rows must have the same value."
        ),
    )
    parser.add_argument(
        "--resource-id",
        help=(
            "Optional: static resourceId to use in the payload. "
            "If not provided, the script will look for a 'resourceId' column "
            "in the CSV. In bulk mode, all rows must resolve to the same resourceId."
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
            cleaned_rows.append(
                {
                    (k.strip() if isinstance(k, str) else k): (
                        v.strip() if isinstance(v, str) else v
                    )
                    for k, v in row.items()
                }
            )

    if not cleaned_rows:
        raise ValueError("CSV file contains no data rows.")

    url = args.api_url
    if args.id_column:
        if args.id_column not in csv_headers:
            raise KeyError(
                f"Configured id-column '{args.id_column}' not found in CSV columns: {csv_headers}"
            )

        id_values = []
        for idx, row in enumerate(cleaned_rows, start=1):
            row_id_value = row.get(args.id_column)
            if row_id_value in (None, ""):
                raise ValueError(
                    f"Row #{idx} is missing id-column '{args.id_column}' required for bulk URL construction."
                )
            id_values.append(row_id_value)

        unique_id_values = list(dict.fromkeys(id_values))
        if len(unique_id_values) != 1:
            raise ValueError(
                f"Bulk mode requires exactly one unique '{args.id_column}' value, but found: {unique_id_values}"
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
            f"[OK] Bulk request sent to {url} "
            f"with {len(payload['resources'])} resources (status {resp.status_code})"
        )
    else:
        print(
            f"[FAIL] Bulk request to {url} (status {resp.status_code}) "
            f"Response: {resp.text[:500]}"
        )


if __name__ == "__main__":
    main()
