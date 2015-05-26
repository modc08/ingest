#!/usr/bin/env python

# pylint: disable=line-too-long

"""List the ACAD object store namespace."""

import argparse, datetime

import tzlocal, yaml

from tabulate import tabulate

from oagr import HCP

parser = argparse.ArgumentParser(
    description="List the ACAD object store namespace.",
    epilog="Remember to populate your YAML configuration with appropriate settings.")

parser.add_argument("-c", "--config", help="Config file (YAML)", default="config.yaml")

local = tzlocal.get_localzone()

def main():
    args = parser.parse_args()

    config = yaml.load(open(args.config, "r"))

    hcp = HCP(config["hcp"])

    object_details = {}
    for o in hcp.list():
        object_details[o["name"]] = o

    output = []
    for obj in sorted(object_details.keys()):
        if object_details[obj]["size"] > 0:
            lastmod = datetime.datetime.fromtimestamp(object_details[obj]["mtime"], local).strftime("%c")
            output.append([obj, object_details[obj]["size"], lastmod])
    print tabulate(output, headers=["name", "size", "last modified"])

if __name__ == "__main__":
    main()
