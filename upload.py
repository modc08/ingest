#!/usr/bin/env python

# pylint: disable=line-too-long

"""Upload arbitrary files into the ACAD object store namespace."""

import argparse, sys

import yaml

from oagr import HCP

parser = argparse.ArgumentParser(
    description="Upload arbitrary files into the ACAD object store namespace.",
    epilog="Remember to populate config.yaml with appropriate settings.")
parser.add_argument("-m", "--md5", help="Name objects according to their MD5 hash.", default=False, action="store_true")
parser.add_argument("filename", help="One or more filenames, space-separated.", nargs='+')

def main():
    args = parser.parse_args()

    config = yaml.load(open("config.yaml", "r"))

    hcp = HCP(config["hcp"])

    for name in args.filename:
        print "Uploading %s..." % name,
        sys.stdout.flush()
        if args.md5:
            key = None
        else:
            key = name
        if hcp.upload(name, key):
            print "done."
        else:
            print "already exists; skipping."

if __name__ == "__main__":
    main()
