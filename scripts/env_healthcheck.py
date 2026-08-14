#!/usr/bin/env python3
"""
Global Environment & Connection Healthcheck Script
Validates all connection details, network targets, and Flink cluster capacity (JobManager & TaskSlots) defined in env.local.
"""

import os
import sys
import json
import socket
import time
import urllib.request
import urllib.error
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def load_env_local():
    """Load env.local if present into os.environ."""
    env_file = Path(__file__).resolve().parent.parent / "env.local"
    if env_file.exists():
        with open(env_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    k, v = k.strip(), v.strip()
                    if k not in os.environ:
                        os.environ[k] = v


def check_tcp(host, port, timeout=2.0):
    try:
        start = time.time()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        res = sock.connect_ex((host, int(port)))
        sock.close()
        elapsed = (time.time() - start) * 1000
        if res == 0:
            return True, f"{elapsed:.1f}ms", "CONNECTED"
        else:
            return False, "-", f"ERR_CODE_{res}"
    except Exception as e:
        return False, "-", str(e)


def check_http(url, timeout=2.0):
    try:
        start = time.time()
        req = urllib.request.Request(url, headers={"User-Agent": "Env-Healthcheck"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = resp.getcode()
            elapsed = (time.time() - start) * 1000
            return (code < 400), f"{elapsed:.1f}ms", f"HTTP_{code}"
    except urllib.error.HTTPError as e:
        elapsed = (time.time() - start) * 1000
        return True, f"{elapsed:.1f}ms", f"HTTP_{e.code}"
    except Exception as e:
        return False, "-", str(e)


def check_flink(url, timeout=2.0):
    try:
        start = time.time()
        endpoint = url.rstrip("/") + "/overview"
        req = urllib.request.Request(endpoint, headers={"User-Agent": "Env-Healthcheck"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            elapsed = (time.time() - start) * 1000
            
            tm_count = data.get("taskmanagers", 0)
            slots_total = data.get("slots-total", 0)
            slots_avail = data.get("slots-available", 0)
            jobs_running = data.get("jobs-running", 0)
            
            if tm_count > 0 and slots_total > 0:
                details = f"Slots: {slots_avail}/{slots_total} avail ({tm_count} TM, {jobs_running} running)"
                return True, f"{elapsed:.1f}ms", details
            else:
                details = f"WARN: 0 TaskManagers registered (0/{slots_total} slots)"
                return False, f"{elapsed:.1f}ms", details
    except Exception as e:
        return False, "-", str(e)


def main():
    load_env_local()

    # Discover configured variables matching env.local
    env_vars = {
        "KAFKA_BOOTSTRAP_SERVERS_LOCAL": os.environ.get("KAFKA_BOOTSTRAP_SERVERS_LOCAL", "localhost:9092"),
        "SCHEMA_REGISTRY_URL_LOCAL": os.environ.get("SCHEMA_REGISTRY_URL_LOCAL", "http://localhost:8084"),
        "KAFKA_CONNECT_URL_LOCAL": os.environ.get("KAFKA_CONNECT_URL_LOCAL", "http://localhost:8083"),
        "FLINK_JOBMANAGER_URL_LOCAL": os.environ.get("FLINK_JOBMANAGER_URL_LOCAL", "http://localhost:8081"),
        "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
        "VOCAB_SERVICE_URL_LOCAL": os.environ.get("VOCAB_SERVICE_URL_LOCAL", "http://localhost:8082"),
        "CLICKHOUSE_PORT": os.environ.get("CLICKHOUSE_PORT", "8123"),
        "CLICKHOUSE_NATIVE_PORT": os.environ.get("CLICKHOUSE_NATIVE_PORT", "9000"),
        "MINIO_ENDPOINT_LOCAL": os.environ.get("MINIO_ENDPOINT_LOCAL", "http://localhost:9000"),
        "MINIO_CONSOLE_URL_LOCAL": os.environ.get("MINIO_CONSOLE_URL_LOCAL", "http://localhost:9001"),
        "REDIS_PORT": os.environ.get("REDIS_PORT", "6379"),
        "LANGFUSE_URL_LOCAL": os.environ.get("LANGFUSE_URL_LOCAL", "http://localhost:4000"),
    }

    # All targets matching env.local sections
    sections = [
        ("STREAMING & FLINKFLOW INFRASTRUCTURE", [
            ("Apache Kafka", env_vars["KAFKA_BOOTSTRAP_SERVERS_LOCAL"], "tcp"),
            ("Schema Registry", env_vars["SCHEMA_REGISTRY_URL_LOCAL"] + "/subjects", "http"),
            ("Kafka Connect", env_vars["KAFKA_CONNECT_URL_LOCAL"] + "/connectors", "http"),
            ("Flink JobManager & Slots", env_vars["FLINK_JOBMANAGER_URL_LOCAL"], "flink"),
            ("PostgreSQL Database", f"127.0.0.1:{env_vars['POSTGRES_PORT']}", "tcp"),
            ("OMOP Vocab Service", env_vars["VOCAB_SERVICE_URL_LOCAL"] + "/health", "http"),
        ]),
        ("LANGFUSE & STORAGE INFRASTRUCTURE", [
            ("ClickHouse HTTP", f"http://127.0.0.1:{env_vars['CLICKHOUSE_PORT']}/ping", "http"),
            ("ClickHouse Native", f"127.0.0.1:{env_vars['CLICKHOUSE_NATIVE_PORT']}", "tcp"),
            ("MinIO S3 Health", f"{env_vars['MINIO_ENDPOINT_LOCAL']}/minio/health/live", "http"),
            ("MinIO Console", f"127.0.0.1:9001", "tcp"),
            ("Redis Cache", f"127.0.0.1:{env_vars['REDIS_PORT']}", "tcp"),
            ("Langfuse Web UI", f"{env_vars['LANGFUSE_URL_LOCAL']}/api/public/health", "http"),
        ])
    ]

    bold = "\033[1m"
    cyan = "\033[1;36m"
    green = "\033[1;32m"
    red = "\033[1;31m"
    yellow = "\033[1;33m"
    reset = "\033[0m"

    print()
    print(f"{cyan}{bold}=========================================================================================={reset}")
    print(f"{cyan}{bold}                         GLOBAL ENVIRONMENT HEALTH CHECK REPORT                           {reset}")
    print(f"{cyan}{bold}=========================================================================================={reset}")

    total_pass = 0
    total_targets = 0

    for section_title, targets in sections:
        print(f"\n{bold}{section_title}{reset}")
        print(f"{bold}{'Service':<26} {'Endpoint':<32} {'Status':<10} {'Latency':<10} {'Details'}{reset}")
        print("-" * 102)
        
        for name, endpoint, check_type in targets:
            total_targets += 1
            if check_type == "tcp":
                parts = endpoint.split(":")
                host, port = parts[0], parts[1] if len(parts) > 1 else 80
                ok, latency, msg = check_tcp(host, port)
            elif check_type == "flink":
                ok, latency, msg = check_flink(endpoint)
            else:
                ok, latency, msg = check_http(endpoint)

            if ok:
                total_pass += 1
                badge = f"{green}✔ PASS{reset}"
            else:
                badge = f"{red}✘ FAIL{reset}"

            ep_display = endpoint
            if len(ep_display) > 30:
                ep_display = ep_display[:27] + "..."
            print(f"{name:<26} {ep_display:<32} {badge:<18} {latency:<10} {msg}")

    print("\n" + "=" * 102)
    if total_pass == total_targets:
        summary_color = green
    elif total_pass > 0:
        summary_color = yellow
    else:
        summary_color = red

    print(f"{bold}Summary:{reset} {summary_color}{total_pass}/{total_targets} global targets reachable from host{reset}")
    print(f"{cyan}{bold}=========================================================================================={reset}\n")


if __name__ == "__main__":
    main()
