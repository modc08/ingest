#!/usr/bin/env python

# pylint: disable=line-too-long,star-args

"""A basic tool for interacting with MyTardis + Hitachi Content Platform (which pretends to be S3)."""

import argparse, shutil, sys
import yaml

from oagr import MyTardis, HCP

parser = argparse.ArgumentParser(
    description="Upload and register an experiment.",
    epilog="Remember to populate config.yaml with appropriate settings.")
parser.add_argument("-D", "--directory", help="Directory", required=True)
parser.add_argument("-t", "--title", help="Title", required=True)
parser.add_argument("-a", "--authors", help="Authors (semi-colon separated, format \"Last, First\")", required=True)
parser.add_argument("-d", "--description", help="Description", required=True)
parser.add_argument("-i", "--institution", help="Institution(s) (optional; can be semicolon separated)")
parser.add_argument("-f", "--force", help="Force overwrite of existing metadata", action="store_true")
parser.add_argument("-r", "--remove", help="Remove experiment directory after successful upload", action="store_true")

def main():
    args = parser.parse_args()

    args.directory = args.directory.strip()
    if args.directory in ["/", ".", ""]:
        print "Error: please use the name of the experiment directory rather than \"%s\"." % args.directory
        sys.exit(1)

    config = yaml.load(open("config.yaml", "r"))

    mytardis = MyTardis(config["mytardis"])
    hcp = HCP(config["hcp"])

    mytardis.upload_metadata(objects=hcp.sync(args.directory), **vars(args))

    if args.remove:
        print "Removing:", args.directory
        shutil.rmtree(args.directory)

    print "Done!"

if __name__ == "__main__":
    main()
