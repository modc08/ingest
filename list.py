#!/usr/bin/env python

# pylint: disable=line-too-long

"""Garbage collection for the ACAD object store namespace."""

import argparse, datetime

import tzlocal, yaml

from oagr import MyTardis, HCP

parser = argparse.ArgumentParser(
    description="Garbage collection for the ACAD object store namespace.",
    epilog="Remember to populate config.yaml with appropriate settings.")
parser.add_argument("-n", "--dry-run", help="Print what actions would be taken, but don't do them.", action="store_true")

local = tzlocal.get_localzone()

def main():
    args = parser.parse_args()

    config = yaml.load(open("config.yaml", "r"))

    mytardis = MyTardis(config["mytardis"])
    hcp = HCP(config["hcp"])

    # Ascertain what we have.

    object_details = {}
    for o in hcp.list():
        object_details[o["name"]] = o
    objects = set(object_details.keys())

    # Obtain references from MyTardis.

    links = set([f["md5sum"] for f in mytardis.fetch("dataset_file").get("objects", [])])

    # Objects - MyTardis => Probably Garbage

    for garbage in sorted(objects - links):
        print garbage, ";", datetime.datetime.fromtimestamp(object_details[garbage]["mtime"], local)

    # TODO
    # - the deletion (!)
    # - handle the dry run arg

if __name__ == "__main__":
    main()
