#!/usr/bin/env python

# pylint: disable=line-too-long,unused-argument

"""A basic tool for managing backups on Hitachi Content Platform (which pretends to be S3)."""
import argparse, glob, sys, os

from oagr import HCP

CONFIG_BACKUP_KEY = "db_backup"

class Backups(HCP):

    def __init__(self, config):
        super(Backups, self).__init__(config)

    def get_latest(self, to_dir):
        objects = self.list(self.base)
        objects.sort(key=lambda item: item['mtime'], reverse=True)
        l = objects[0]
        try:
            k = self.bucket.lookup(l["name"])
            k.get_contents_to_filename("%s/db_backup_%s.gz" % (to_dir, l["mtime"]))
            print "Downloaded and saved %s/db_backup_%s.gz" % (to_dir, l["mtime"])
        except Exception as e:
            print "Failed to download %s" % l["name"]
            print e

# check backup folder and upload them if the have not in the store
def upload_backup(config, remove=False):
    hcp = Backups(config)
    if 'directory' in config:
        directory = config["directory"]
    else:
        directory = '/tmp'
    print "Check if there is any candidate in %s, has to be gz files" % directory
    for datafile in glob.iglob("%s/*.gz" % directory):
        print "Uploading %s..." % datafile
        if hcp.upload(datafile, None):
            print "Uploaded: %s" % datafile
        else:
            print "Already exists; skipping."
        if remove:
            print "%s removed" % datafile
            os.remove(datafile)

# get the latest backup, save into a directory
def retrieve_backup(config):
    if 'directory' in config:
        directory = config["directory"]
    else:
        directory = '/tmp'
    hcp = Backups(config)
    hcp.get_latest(directory)

parser = argparse.ArgumentParser(
    description="Back up whatever found in a folder.",
    epilog="Remember to populate config.yaml with appropriate settings.")
parser.add_argument("-c", "--config", help="Config file path (optional). Default is ./config.yaml")
parser.add_argument("-a", "--action", help="Action to be taken: either backup or retrieve the latest backup. ", required=True, choices=['upload', 'retrieve'])
parser.add_argument("-r", "--remove", help="Remove experiment directory after successful upload", action="store_true")

if __name__ == "__main__":
    args = parser.parse_args()

    if args.config:
        config_file = args.config
    else:
        config_file = "config.yaml"

    import yaml
    config = yaml.load(open(config_file, "r"))[CONFIG_BACKUP_KEY]
    sys.stdout.flush()
    if args.action == "upload":
        print "Backup"
        upload_backup(config, args.remove)
    else:
        print "Get the latest backup from %s of bucket %s in object store" % (config["base"], config["bucket"])
        retrieve_backup(config)
