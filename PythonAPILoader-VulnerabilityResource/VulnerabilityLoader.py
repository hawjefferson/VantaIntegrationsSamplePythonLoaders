#!/usr/bin/env python3
"""
Read a CSV file and send each row as a JSON body via PUT to an API endpoint.

Each row produces a payload like:

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


def send_row_put(
    url: str,
    payload: Dict[str, Any],
    headers: Dict[str, str],
    timeout: int = 10,
) -> requests.Response:
    """Send a single payload as JSON via PUT."""
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

    # Booleans
    if v.lower() == "true":
        return True
    if v.lower() == "false":
        return False

    # Try JSON (for arrays/objects, e.g. ["PUSH_PROMPT"])
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

    # Try JSON array first
    if v.startswith("[") and v.endswith("]"):
        try:
            parsed = json.loads(v)
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
        except json.JSONDecodeError:
            pass

    # Fall back to comma-separated list or single value
    if "," in v:
        return [part.strip() for part in v.split(",") if part.strip()]

    # Single value
    return [v]


def build_payload(
    csv_row: Dict[str, Any],
    headers: List[str],
    resource_id_override: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build the JSON body to send for each request:

    {
      "resources": [ { ...fields from CSV... } ],
      "resourceId": "<from CLI or CSV>"
    }

    Special handling:
      - resourceId: from --resource-id or CSV 'resourceId'
      - mfaMethods: always an array
      - mfaEnabled: always a boolean
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
            continue  # handled separately

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

            # Build final payload using your JSON template shape
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
