#!/usr/bin/env python

# pylint: disable=line-too-long,unused-argument

"""A basic tool for managing backups on Hitachi Content Platform (which pretends to be S3)."""
import argparse, glob, sys, os, datetime, tzlocal
import logging

from oagr import HCP

logging.basicConfig(filename='database_backups.log', \
    format='[%(asctime)s] %(levelname)-7s %(message)s', \
    datefmt='%d/%b/%Y %H:%M:%S')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

CONFIG_BACKUP_KEY = "db_backup"

class Backups(HCP):

    def __init__(self, config):
        super(Backups, self).__init__(config)

    def get_latest(self, to_dir):
        objects = self.list(self.base)
        objects.sort(key=lambda item: item['mtime'], reverse=True)
        self.__retrive__(objects[0], to_dir)

    def get_dated(self, to_dir, q_date):
        # Use q_date to check if a backup was uploaded within one day of it and retrieve it if found
        #q_date is in local timezone, has to be in YYYYMMDD format
        #checking from the latest backwards
        local = tzlocal.get_localzone()
        dl = local.localize(datetime.datetime.strptime(q_date, '%Y%m%d'))
        qmtime = (dl-self.epoch).total_seconds()

        objects = self.list(self.base)
        objects.sort(key=lambda item: item['mtime'], reverse=True)
        for o in objects:
            if abs(o["mtime"] - qmtime) < 86400:
                self.__retrive__(o, to_dir)
                return
        logger.info("No backup created within one day around %s" % q_date)

    def __retrive__(self, kd, to_dir):
        try:
            k = self.bucket.lookup(kd["name"])
            k.get_contents_to_filename("%s/db_backup_%s.gz" % (to_dir, kd["mtime"]))
            logger.info("Downloaded and saved %s/db_backup_%s.gz" % (to_dir, kd["mtime"]))
        except Exception as e:
            logger.error("Failed to download %s" % kd["name"])
            logger.error(e)


# check backup folder and upload them if the have not in the store
def upload_backup(config, remove=False):
    hcp = Backups(config)
    if 'directory' in config:
        directory = config["directory"]
    else:
        directory = '/tmp'
    logger.debug("Check if there is any candidate in %s, has to be gz files" % directory)
    for datafile in glob.iglob("%s/*.gz" % directory):
        logger.debug("Uploading %s..." % datafile)
        if hcp.upload(datafile, None):
            logger.info("Uploaded: %s" % datafile)
        else:
            logger.debug("Already exists; skipping.")
        if remove:
            logger.info("%s removed" % datafile)
            os.remove(datafile)
    logger.info("Uploading of backups completely")

# get the latest backup, save into a directory
def retrieve_backup(config, dated):
    if 'directory' in config:
        directory = config["directory"]
    else:
        directory = '/tmp'
    hcp = Backups(config)
    if dated:
        hcp.get_dated(directory, dated)
    else:
        hcp.get_latest(directory)
    logger.info("Retrieving backup completely")

parser = argparse.ArgumentParser(
    description="Back up whatever found in a folder.",
    epilog="Remember to populate config.yaml with appropriate settings.")
parser.add_argument("-c", "--config", help="Config file path (optional). Default is ./config.yaml")
parser.add_argument("-a", "--action", help="Action to be taken: either backup or retrieve the latest backup. ", required=True, choices=['upload', 'retrieve'])
parser.add_argument("-r", "--remove", help="Remove experiment directory after successful upload", action="store_true")
parser.add_argument("-d", "--dated", help="Date when a backup was uploaded. Has to be in YYYYMMDD format")

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
        logger.info("Run backup")
        upload_backup(config, args.remove)
    else:
        logger.info("Get a backup from %s of bucket %s in object store" % (config["base"], config["bucket"]))
        retrieve_backup(config, args.dated)
