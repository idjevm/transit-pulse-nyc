"""Register Confluent's Real-Time Context Engine (RTCE) as an MCP server.

Exposes live MTA transit topics (mta_vehicle_positions, mta_headway_alerts)
as an MCP tool so AI coding agents (Claude Code, Cursor, Windsurf, Codex) can query
the live fleet directly.

Usage:
    python scripts/setup_rtce.py
    python scripts/setup_rtce.py --dry-run
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import shlex
import subprocess
import sys

from dotenv import load_dotenv

load_dotenv()

_SERVER_NAME = "transit-pulse-mcp"


def get_confluent_context() -> dict[str, str]:
    """Inspect current Confluent CLI context to derive RTCE endpoint and IDs."""
    try:
        org_out = subprocess.run(
            ["confluent", "organization", "describe", "-o", "json"],
            capture_output=True, text=True, check=True
        )
        org_id = json.loads(org_out.stdout).get("id", "")
    except Exception:
        org_id = ""

    try:
        env_out = subprocess.run(
            ["confluent", "environment", "list", "-o", "json"],
            capture_output=True, text=True, check=True
        )
        envs = json.loads(env_out.stdout)
        env_id = next((e["id"] for e in envs if e.get("is_current") or e.get("name") == "default"), "")
    except Exception:
        env_id = ""

    try:
        cluster_out = subprocess.run(
            ["confluent", "kafka", "cluster", "list", "-o", "json"],
            capture_output=True, text=True, check=True
        )
        clusters = json.loads(cluster_out.stdout)
        cluster = next((c for c in clusters if c.get("is_current") or c.get("name") == "cluster_0"), {})
        cluster_id = cluster.get("id", "")
        region = cluster.get("region", "us-east-1")
    except Exception:
        cluster_id = ""
        region = "us-east-1"

    endpoint = (
        f"https://mcp.{region}.aws.confluent.cloud/mcp/v1/context-engine"
        f"/organizations/{org_id}/environments/{env_id}/kafka-clusters/{cluster_id}"
        if (org_id and env_id and cluster_id) else ""
    )

    return {
        "org_id": org_id,
        "env_id": env_id,
        "cluster_id": cluster_id,
        "region": region,
        "endpoint": endpoint,
    }


def mint_global_key() -> tuple[str, str]:
    """Mint a Global API key for RTCE MCP querying."""
    try:
        result = subprocess.run(
            ["confluent", "api-key", "create", "--resource", "global", "--description", "transit-pulse-rtce", "-o", "json"],
            capture_output=True, text=True, check=True
        )
        data = json.loads(result.stdout)
        return data.get("api_key", "") or data.get("key", ""), data.get("api_secret", "") or data.get("secret", "")
    except Exception as e:
        print(f"Warning: could not automatically mint Global API key ({e}).")
        return "", ""


def main() -> None:
    parser = argparse.ArgumentParser(description="Configure Confluent RTCE MCP for AI agents")
    parser.add_argument("--dry-run", action="store_true", help="Print configuration without running commands")
    args = parser.parse_args()

    ctx = get_confluent_context()
    endpoint = ctx["endpoint"]

    if not endpoint:
        print("Error: Could not resolve Confluent Cloud organization, environment, or cluster.")
        sys.exit(1)

    print(f"RTCE Endpoint: {endpoint}")
    key, secret = mint_global_key()

    if key and secret:
        token = base64.b64encode(f"{key}:{secret}".encode()).decode()
        claude_cmd = [
            "claude", "mcp", "add", "--transport", "http", _SERVER_NAME, endpoint,
            "--header", f"Authorization: Basic {token}",
        ]
        print("\nTo connect Claude Code:")
        print(f"  {shlex.join(claude_cmd)}")

        print("\nTo connect Cursor / Windsurf / Codex (HTTP Server config):")
        print(f"  Name:    {_SERVER_NAME}")
        print(f"  URL:     {endpoint}")
        print(f"  Headers: Authorization: Basic {token}")

        if not args.dry_run and shutil_which("claude"):
            print("\nAttempting automatic registration with Claude Code...")
            subprocess.run(["claude", "mcp", "remove", _SERVER_NAME, "-s", "local"], capture_output=True)
            res = subprocess.run(claude_cmd, capture_output=True, text=True)
            if res.returncode == 0:
                print("Successfully registered with Claude Code!")
            else:
                print(f"Notice: {res.stderr.strip()}")
    else:
        print("\nNote: Create a Global API key with `confluent api-key create --resource global` to authenticate.")


def shutil_which(cmd: str) -> bool:
    import shutil
    return shutil.which(cmd) is not None


if __name__ == "__main__":
    main()

