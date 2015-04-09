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

    def get_version(self, to_dir, backupname):
    # run list.py and find the file name of a backup,
    # use the name without base as backupname for retrieving
        if self.exists(backupname, True):
            self.__retrive__({'name': self.base + backupname}, to_dir)
            try:
                k = self.bucket.lookup(self.base + backupname)
                k.get_contents_to_filename("%s/%s" % (to_dir, backupname))
                logger.info("Downloaded and saved %s/%s" % (to_dir, backupname))
            except Exception as e:
                logger.error("Failed to download %s" % backupname)
                logger.error(e)
        else:
            logger.info("Cannot find %s in store. Check name." % backupname)

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

    def mark_latest(self, key_name):
        return self.bucket.copy_key(self.base + "latest", self.bucket.name, key_name) is not None

    def __retrive__(self, kd, to_dir):
        try:
            k = self.bucket.lookup(kd["name"])
            k.get_contents_to_filename("%s/retrieved_backup_%s.gz" % (to_dir, kd["mtime"]))
            logger.info("Downloaded and saved %s/retrieved_backup_%s.gz" % (to_dir, kd["mtime"]))
        except Exception as e:
            logger.error("Failed to download %s" % kd["name"])
            logger.error(e)


# check backup folder and upload them if the have not in the store
# copy_key
def upload_backup(config, remove=False):
    hcp = Backups(config)
    if 'directory' in config:
        directory = config["directory"]
    else:
        directory = '/tmp'
    logger.debug("Check if there is any candidate in %s, has to be gz files" % directory)
    # see db_bakup.sh for what name pattern is used in producing backups
    latest_not_marked = True
    base = hcp.base
    for datafile in sorted(glob.iglob("%s/dbbackup_*.gz" % directory), key=os.path.getmtime, reverse=True):
        kname = base + datafile.replace("dbbackup_", "")
        logger.debug("Uploading %s..." % datafile)
        if hcp.upload(datafile, kname):
            logger.info("Uploaded: %s as %s" % (datafile, kname))
            if latest_not_marked:
                hcp.upload(datafile, hcp.base + "latest")
                #~ if not hcp.mark_latest(kname):
                    #~ logger.error("Could not mark %s as the latest" % kname)
                latest_not_marked = False
        else:
            logger.debug("Already exists; skipping.")
        if remove:
            logger.info("%s removed" % datafile)
            os.remove(datafile)
    logger.info("Uploading of backups completely")

# get the latest backup, save into a directory
def retrieve_backup(config, backupname):
    if 'directory' in config:
        directory = config["directory"]
    else:
        directory = '/tmp'
    hcp = Backups(config)
    if backupname == 'latest':
        hcp.get_latest(directory)
    else:
        hcp.get_version(directory, backupname)
    logger.info("Retrieving backup completely")

parser = argparse.ArgumentParser(
    description="Back up whatever found in a folder.",
    epilog="Remember to populate config.yaml with appropriate settings.")
parser.add_argument("-c", "--config", help="Config file path (optional). Default is ./config.yaml")
parser.add_argument("-a", "--action", help="Action to be taken: either backup or retrieve the latest backup. ", required=True, choices=['upload', 'retrieve'])
parser.add_argument("-r", "--remove", help="Remove experiment directory after successful upload", action="store_true")
parser.add_argument("-n", "--backupname", help="Name of a backup. When not set, the latest backup is used. Otherwise, it has to be the exact name of a backup")

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
        retrieve_backup(config, args.backupname)
