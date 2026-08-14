#!/usr/bin/env python3
"""
Universal Confluent Schema Registry CLI for Flink Pipelines.
Discovers and registers schemas dynamically without hardcoded names.
Supports JSON Schema (.json), Apache Avro (.avsc), and Protobuf (.proto).
"""

import os
import sys
import json
import argparse
import fnmatch
import urllib.request
import urllib.error
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def get_schema_registry_url():
    """Retrieve Schema Registry URL from environment or fallback to local port 8084."""
    url = os.environ.get(
        "SCHEMA_REGISTRY_URL_LOCAL",
        os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8084")
    )
    return url.rstrip("/")


def detect_schema_type(file_path: Path, override_type: str = None) -> str:
    """Infer Confluent schema type from file extension or explicit override."""
    if override_type:
        return override_type.upper()
    suffix = file_path.suffix.lower()
    if suffix == ".json":
        return "JSON"
    elif suffix in (".avsc", ".avro"):
        return "AVRO"
    elif suffix == ".proto":
        return "PROTOBUF"
    return "JSON"


def register_single_schema(registry_url: str, subject: str, schema_file: Path, schema_type: str = "JSON") -> bool:
    """POST schema payload to Confluent Schema Registry."""
    if not schema_file.exists():
        print(f"❌ Schema file not found: {schema_file}", file=sys.stderr)
        return False

    with open(schema_file, "r", encoding="utf-8") as f:
        raw_schema = f.read()

    # JSON Schema requires stringified JSON inside schema property
    payload = {
        "schemaType": schema_type,
        "schema": json.dumps(json.loads(raw_schema)) if schema_type == "JSON" else raw_schema
    }

    url = f"{registry_url}/subjects/{subject}/versions"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            schema_id = data.get("id")
            print(f"✅ Registered '{subject}' ({schema_type}) with Schema Registry (ID: {schema_id})")
            return True
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"❌ Failed to register '{subject}' (HTTP {e.code}): {body}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"❌ Connection error to Schema Registry at {registry_url}: {e}", file=sys.stderr)
        return False


def find_schemas(schemas_dir: Path, target: str = None):
    """
    Find schemas in schemas_dir based on target path or glob pattern.
    If target is None, returns all valid schemas in schemas_dir.
    """
    if not schemas_dir.exists():
        return []

    valid_extensions = {".json", ".avsc", ".avro", ".proto"}
    all_files = [p for p in schemas_dir.iterdir() if p.is_file() and p.suffix.lower() in valid_extensions]

    if not target or target.strip() in ("", "*", "all"):
        return all_files

    target = target.strip()

    # Direct file check
    direct_path = Path(target)
    if direct_path.exists() and direct_path.is_file():
        return [direct_path]
    
    in_dir_path = schemas_dir / target
    if in_dir_path.exists() and in_dir_path.is_file():
        return [in_dir_path]

    # Pattern match against filename
    pattern = target if any(c in target for c in "*?[]") else f"*{target}*"
    matched = [p for p in all_files if fnmatch.fnmatch(p.name, pattern) or fnmatch.fnmatch(p.stem, pattern)]
    return matched


def main():
    parser = argparse.ArgumentParser(description="Universal Schema Registry CLI for Flink Pipelines")
    parser.add_argument("target", nargs="?", default=None, help="Schema file path, subject name, or glob pattern (e.g. 'omop*', 'schemas/user-value.json')")
    parser.add_argument("--subject", default=None, help="Explicit subject name (default: filename without extension)")
    parser.add_argument("--type", default=None, choices=["JSON", "AVRO", "PROTOBUF"], help="Explicit schema type (default: auto-detected from extension)")
    parser.add_argument("--url", default=None, help="Schema Registry URL (default: from env.local or http://localhost:8084)")
    args = parser.parse_args()

    registry_url = (args.url or get_schema_registry_url()).rstrip("/")
    schemas_dir = Path(__file__).resolve().parent.parent / "schemas"

    matched_schemas = find_schemas(schemas_dir, args.target)

    if not matched_schemas:
        print(f"⚠️  No schema files found matching '{args.target or 'all'}' in {schemas_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"📡 Connecting to Schema Registry at {registry_url}...")
    print(f"📁 Found {len(matched_schemas)} schema file(s) to process:\n")

    success = True
    for schema_file in matched_schemas:
        subject = args.subject if (len(matched_schemas) == 1 and args.subject) else schema_file.stem
        stype = detect_schema_type(schema_file, args.type if len(matched_schemas) == 1 else None)
        ok = register_single_schema(registry_url, subject, schema_file, stype)
        if not ok:
            success = False

    if success:
        print(f"\n🎉 Schema registration completed successfully!")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
