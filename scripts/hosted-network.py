#!/usr/bin/env python3
"""Provision authenticated private any-sync services on your own server.

Only sync TCP/QUIC ports are published. Storage, metrics and admin ports remain
inside Docker. The advertised hostname and wildcard bind are explicit choices;
there is no connection to an Anytype-operated network.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess

spec=importlib.util.spec_from_file_location("private_network",Path(__file__).with_name("private-network.py"))
base=importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)
PROJECT="sync-core-private"
SERVICES=["node-1","node-2","node-3","coordinator","filenode","consensusnode"]

def constrain(config):
    for name,service in config["services"].items():
        service.pop("ports",None)
        if name.startswith("any-sync-") and name[9:] in SERVICES:
            index=SERVICES.index(name[9:])+1
            service["ports"]=[{"target":19000+index,"published":str(19000+index),"host_ip":"0.0.0.0","protocol":"tcp"},
                              {"target":19100+index,"published":str(19100+index),"host_ip":"0.0.0.0","protocol":"udp"}]
    return config

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action",choices=["prepare","start","status","stop"])
    parser.add_argument("--directory",type=Path,required=True)
    parser.add_argument("--advertise-host",help="administrator-controlled hostname/IP reachable by devices")
    args=parser.parse_args()
    os.umask(0o077)
    directory=args.directory.expanduser().resolve()
    repo=Path(__file__).resolve().parents[1]
    if directory.is_relative_to(repo) or any((p/".git").exists() for p in directory.parents):
        raise ValueError("runtime directory must be outside source checkouts")
    if args.action=="prepare":
        host=args.advertise_host or ""
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]{0,252}",host):
            raise ValueError("an explicit valid advertised hostname or IPv4 address is required")
        directory.mkdir(parents=True,exist_ok=False,mode=0o700)
        with (directory/"prepare.log").open("x") as log:
            base.run(["git","init","--quiet","."],directory,log)
            base.run(["git","remote","add","origin",base.UPSTREAM],directory,log)
            base.run(["git","fetch","--depth","1","origin",base.REVISION],directory,log)
            base.run(["git","checkout","--detach","FETCH_HEAD"],directory,log)
            environment=base.environment((directory/".env.example").read_text())
            environment=environment.replace('EXTERNAL_LISTEN_HOSTS="127.0.0.1"',f'EXTERNAL_LISTEN_HOSTS="{host}"')
            base.private_write(directory/".env",environment)
            dockerfile=directory/"Dockerfile-any-sync-init"
            text=dockerfile.read_text().replace("FROM ghcr.io/anyproto/any-sync-tools\n",f"FROM ghcr.io/anyproto/any-sync-tools@{base.TOOLS}\n")
            # Bound descriptor enumeration in apt/dpkg on older Docker hosts.
            text=text.replace("RUN set -eux;", "RUN ulimit -n 1024; set -eux;")
            dockerfile.write_text(text.replace("yq/releases/latest/download/","yq/releases/download/v4.44.3/"))
            # A standalone resolved file supports older Compose without !override.
            raw=subprocess.check_output(["docker","compose","--project-name",PROJECT,"-f","docker-compose.yml","config","--format","json"],cwd=directory,stderr=log)
            base.private_write(directory/"private.resolved.json",json.dumps(constrain(json.loads(raw)),indent=2))
            base.run(["docker","compose","-p",PROJECT,"-f","private.resolved.json","config","--quiet"],directory,log)
        with (directory/".git/info/exclude").open("a") as file:
            file.write("\n.env\nprivate.resolved.json\n.hosted-network\n*.log\netc/\nstorage/\n")
        base.private_write(directory/".hosted-network",base.REVISION+"\n")
        print(f"Prepared private network at {directory}; no services started.")
        return
    if (directory/".hosted-network").read_text().strip()!=base.REVISION:
        raise ValueError("not a matching hosted private network")
    command=["docker","compose","-p",PROJECT,"-f","private.resolved.json"]
    if args.action=="start":
        with (directory/"start.log").open("a") as log:
            base.run(command+["up","-d","--wait","--wait-timeout","300"],directory,log)
        print("Private services started; client configuration is in etc/client.yml.")
    else:
        base.run(command+(["ps"] if args.action=="status" else ["stop"]),directory)

if __name__=="__main__":
    main()
