#!/usr/bin/env python3
"""
Flinkflow Multi-Environment Profile Manager & UI API Backend.
------------------------------------------------------------
Allows developers, DevOps, and the AI Studio UI to:
1. List available environment profiles (Local, Staging, Production).
2. Read/Update environment endpoints and configurations dynamically.
3. Export environment variables for local runs or CI/CD API payloads.
"""

import argparse
import json
import os
import sys
import yaml
from pathlib import Path
from typing import Dict, Any

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
CONFIG_FILE = ROOT_DIR / "config" / "environments.yaml"


def load_config() -> Dict[str, Any]:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Environments config not found: {CONFIG_FILE}")
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_config(config: Dict[str, Any]) -> None:
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


def get_environment_profile(env_name: str) -> Dict[str, Any]:
    config = load_config()
    envs = config.get("environments", {})
    if env_name not in envs:
        raise ValueError(f"Environment '{env_name}' not found. Available: {list(envs.keys())}")
    return envs[env_name]


def export_env_vars(env_name: str) -> Dict[str, str]:
    """Flattens environment profile into standard Flinkflow runtime environment variables."""
    profile = get_environment_profile(env_name)
    endpoints = profile.get("endpoints", {})
    topics = profile.get("topics", {})
    llm = profile.get("llm", {})
    cluster = profile.get("cluster", {})

    env_map = {
        "FLINKFLOW_ENV": env_name,
        "KAFKA_BOOTSTRAP_SERVERS": endpoints.get("kafka_bootstrap", "localhost:9092"),
        "SCHEMA_REGISTRY_URL": endpoints.get("schema_registry_url", "http://localhost:8084"),
        "POSTGRES_URL": endpoints.get("postgres_url", "jdbc:postgresql://localhost:5432/outbox_demo"),
        "VOCAB_SERVICE_URL": endpoints.get("vocab_service_url", "http://localhost:8082"),
        "OMOP_EVENTS_TOPIC": topics.get("omop_events_topic", "omop-standard-events"),
        "OMOP_AI_ALERTS_TOPIC": topics.get("omop_ai_alerts_topic", "omop-cdss-ai-alerts"),
        "LLM_PROVIDER": llm.get("provider", "gemini"),
        "LLM_MODEL": llm.get("model", "gemini-2.0-flash"),
        "K8S_NAMESPACE": cluster.get("namespace", "flink-staging"),
    }
    return env_map


def main():
    parser = argparse.ArgumentParser(description="Flinkflow Environment Profile Manager")
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")

    # list
    subparsers.add_parser("list", help="List all environments as JSON (for Studio UI)")

    # get
    get_parser = subparsers.add_parser("get", help="Get a specific environment configuration")
    get_parser.add_argument("env_name", help="Environment name (local, staging, production)")

    # export
    export_parser = subparsers.add_parser("export", help="Export environment variables for a target")
    export_parser.add_argument("env_name", help="Environment name (local, staging, production)")
    export_parser.add_argument("--format", choices=["json", "shell"], default="json")

    # set-active
    set_parser = subparsers.add_parser("set-active", help="Set the default active environment")
    set_parser.add_argument("env_name", help="Environment name")

    # update-field
    update_parser = subparsers.add_parser("update", help="Update a specific field in an environment")
    update_parser.add_argument("env_name", help="Environment name")
    update_parser.add_argument("key_path", help="Dot-separated path (e.g. endpoints.kafka_bootstrap)")
    update_parser.add_argument("value", help="New value")

    args = parser.parse_args()

    if args.action == "list":
        config = load_config()
        print(json.dumps(config, indent=2))

    elif args.action == "get":
        profile = get_environment_profile(args.env_name)
        print(json.dumps(profile, indent=2))

    elif args.action == "export":
        vars_map = export_env_vars(args.env_name)
        if args.format == "shell":
            for k, v in vars_map.items():
                print(f'export {k}="{v}"')
        else:
            print(json.dumps(vars_map, indent=2))

    elif args.action == "set-active":
        config = load_config()
        if args.env_name not in config.get("environments", {}):
            print(f"Error: Unknown environment {args.env_name}", file=sys.stderr)
            sys.exit(1)
        config["active_environment"] = args.env_name
        save_config(config)
        print(f"✅ Active environment set to: {args.env_name}")

    elif args.action == "update":
        config = load_config()
        if args.env_name not in config.get("environments", {}):
            print(f"Error: Unknown environment {args.env_name}", file=sys.stderr)
            sys.exit(1)

        keys = args.key_path.split(".")
        curr = config["environments"][args.env_name]
        for k in keys[:-1]:
            curr = curr.setdefault(k, {})
        curr[keys[-1]] = args.value
        save_config(config)
        print(f"✅ Updated {args.env_name}.{args.key_path} = {args.value}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
