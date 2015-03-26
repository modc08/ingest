#!/usr/bin/env python

# pylint: disable=line-too-long

"""Garbage collection for the ACAD object store namespace."""

import argparse, datetime

import tzlocal, yaml

from oagr import MyTardis, HCP

parser = argparse.ArgumentParser(
    description="Garbage collection for the ACAD object store namespace. Note: default mode is to do a dry run.",
    epilog="Remember to populate config.yaml with appropriate settings.")
parser.add_argument("-y", "--yes-really", help="Yes, delete objects. Really.", action="store_true")

local = tzlocal.get_localzone()

def main():
    args = parser.parse_args()

    config = yaml.load(open("config.yaml", "r"))

    mytardis = MyTardis(config["mytardis"])
    hcp = HCP(config["hcp"])

    # Ascertain what we have.

    print "Listing objects...",
    object_details = {}
    for o in hcp.list():
        object_details[o["name"]] = o
    objects = set(object_details.keys())
    print "done."

    # Obtain references from MyTardis.

    print "Listing references...",
    links = set([f["md5sum"] for f in mytardis.fetch("dataset_file").get("objects", [])])
    print "done."

    if len(links) == 0:
        print "MyTardis has no object store references."
        print "For safety reasons I won't wipe the entire object store; exiting."
        return

    # Objects - MyTardis => Probably Garbage
    # Yes, we completely ignore concurrency -- but for this use case, not a problem.

    unreferenced = sorted(objects - links)
    live = links & objects
    dead = links - objects

    if len(unreferenced) == 0:
        print "Nothing to clean up; exiting."
        return

    print "MyTardis references %i objects. The store contains %i objects." % (len(links), len(objects))
    print "Live references: %i" % len(live)
    print "Dead references (should be zero!): %i" % len(dead)
    print "Unreferenced objects: %i" % len(unreferenced)
    print ""

    for garbage in unreferenced:
        lastmod = datetime.datetime.fromtimestamp(object_details[garbage]["mtime"], local).strftime("%c")
        print "Deleting %s [%s] ..." % (garbage, lastmod),
        if args.yes_really:
            hcp.delete(garbage)
            print "done."
        else:
            print "(skipping)"

if __name__ == "__main__":
    main()
