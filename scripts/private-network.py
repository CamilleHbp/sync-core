#!/usr/bin/env python3
"""Run an isolated, loopback-only, self-hosted any-sync development network.

Generated keys and databases live outside the checkout. There is no public
network fallback, version lookup API, account import, or destructive reset.
"""
import argparse
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys

UPSTREAM = "https://github.com/anyproto/any-sync-dockercompose.git"
REVISION = "8035d68d3ac724f7e3e3ea3fcc5da94aaedc35a7"
TOOLS = "sha256:ee6cf65958ee4e9b558c3973802b7fbd64b3a66c7cf78d64d1a08fb9de0af510"
PROJECT = "sync-core-private-dev"


def run(args, directory, log=None):
    return subprocess.run(args, cwd=directory, check=True, stdout=log, stderr=log)


def private_write(path, text):
    with path.open("x") as file:
        file.write(text)
    path.chmod(0o600)


def environment(template):
    overrides = {
        "EXTERNAL_LISTEN_HOSTS": '"127.0.0.1"',
        "AWS_ACCESS_KEY_ID": "GK" + secrets.token_hex(12),
        "AWS_SECRET_ACCESS_KEY": secrets.token_hex(32),
        "GARAGE_RPC_SECRET": secrets.token_hex(32),
        "ANY_SYNC_TOOLS_VERSION": "latest@" + TOOLS,
        "MONGO_1_PORT": "27091",
        "REDIS_PORT": "6391",
    }
    for index, service in enumerate(
        ["NODE_1", "NODE_2", "NODE_3", "COORDINATOR", "FILENODE", "CONSENSUSNODE"], 1
    ):
        overrides[f"ANY_SYNC_{service}_PORT"] = str(19000 + index)
        overrides[f"ANY_SYNC_{service}_QUIC_PORT"] = str(19100 + index)
        overrides[f"ANY_SYNC_{service}_METRIC_ADDR"] = f"127.0.0.1:{19200 + index}"
        if index <= 3:
            overrides[f"ANY_SYNC_{service}_API_SERVER_ADDR"] = f"127.0.0.1:{19300 + index}"
    for key, value in overrides.items():
        template, count = re.subn(rf"^{key}=.*$", lambda _: f"{key}={value}", template, flags=re.M)
        if count != 1:
            raise ValueError(f"expected exactly one upstream setting: {key}")
    return template


def override():
    # !override replaces, rather than appends to, upstream's wildcard mappings.
    lines = ["services:"]
    names = ["node-1", "node-2", "node-3", "coordinator", "filenode", "consensusnode"]
    for index, name in enumerate(names, 1):
        lines += [f"  any-sync-{name}:", "    ports: !override",
                  f'      - "127.0.0.1:{19000 + index}:{19000 + index}/tcp"',
                  f'      - "127.0.0.1:{19100 + index}:{19100 + index}/udp"']
    # DB, metrics and administrative endpoints are accessible only inside Docker.
    for name in ["mongo-1", "redis"]:
        lines += [f"  {name}:", "    ports: !override []"]
    return "\n".join(lines) + "\n"


def compose(directory, *args, log=None):
    return run(["docker", "compose", "--project-name", PROJECT,
                "-f", "docker-compose.yml", "-f", "private.override.yml", *args], directory, log)


def check_ports(directory):
    data = subprocess.check_output(
        ["docker", "compose", "--project-name", PROJECT, "-f", "docker-compose.yml",
         "-f", "private.override.yml", "config", "--format", "json"], cwd=directory
    )
    config = json.loads(data)
    for service in config["services"].values():
        for port in service.get("ports", []):
            if port.get("host_ip") != "127.0.0.1":
                raise ValueError("refusing to expose a non-loopback service")
    # Do not print the resolved config: it contains generated storage credentials.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "start", "status", "stop"])
    parser.add_argument("--directory", type=Path, required=True,
                        help="private runtime directory outside all source checkouts")
    args = parser.parse_args()
    os.umask(0o077)
    directory = args.directory.expanduser().resolve()
    repo = Path(__file__).resolve().parents[1]
    if directory.is_relative_to(repo) or any((p / ".git").exists() for p in directory.parents):
        raise ValueError("runtime directory must be outside source checkouts")
    if args.action == "prepare":
        # Never replace a pre-existing network or its keys.
        directory.mkdir(parents=True, exist_ok=False, mode=0o700)
        with (directory / "prepare.log").open("x") as log:
            run(["git", "init", "--quiet", "."], directory, log)
            run(["git", "remote", "add", "origin", UPSTREAM], directory, log)
            run(["git", "fetch", "--depth", "1", "origin", REVISION], directory, log)
            run(["git", "checkout", "--detach", "FETCH_HEAD"], directory, log)
        private_write(directory / ".env", environment((directory / ".env.example").read_text()))
        private_write(directory / "private.override.yml", override())
        dockerfile = directory / "Dockerfile-any-sync-init"
        text = dockerfile.read_text().replace("FROM ghcr.io/anyproto/any-sync-tools\n",
                                             f"FROM ghcr.io/anyproto/any-sync-tools@{TOOLS}\n")
        text = text.replace("yq/releases/latest/download/", "yq/releases/download/v4.44.3/")
        dockerfile.write_text(text)
        # Keep even generated state unstageable in the isolated upstream clone.
        with (directory / ".git/info/exclude").open("a") as file:
            file.write("\n.env\nprivate.override.yml\n*.log\netc/\nstorage/\n")
        private_write(directory / ".private-network", REVISION + "\n")
        check_ports(directory)
        print(f"Prepared private network in {directory}; no services started.")
        return
    # This exact marker prevents accidentally controlling another deployment.
    if (directory / ".private-network").read_text().strip() != REVISION:
        raise ValueError("not a matching sync-core private development network")
    if args.action == "start":
        check_ports(directory)
        log_path = directory / "start.log"
        with log_path.open("a") as log:
            compose(directory, "up", "-d", "--wait", "--wait-timeout", "240", log=log)
        print(f"Private network started; client network configuration: {directory / 'etc/client.yml'}")
    elif args.action == "stop":
        compose(directory, "stop")
    else:
        compose(directory, "ps")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        # subprocess failures report the command/exit code, never captured config.
        print(f"Private network blocked: {error}", file=sys.stderr)
        sys.exit(1)
