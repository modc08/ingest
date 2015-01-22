#!/usr/bin/env python

"""A basic tool for interacting with MyTardis + Hitachi Content Platform (which pretends to be S3)."""

import argparse, base64, glob, hashlib, json, os
import boto, requests, yaml

from boto.s3.connection import S3Connection

parser = argparse.ArgumentParser(
    description = "Upload and register an experiment.",
    epilog = "Remember to populate config.yaml with appropriate settings.")
parser.add_argument("-D", "--dir", help = "Directory", required = True)
parser.add_argument("-t", "--title", help = "Title", required = True)
parser.add_argument("-d", "--description", help = "Description", required = True)
parser.add_argument("-i", "--institution", help = "Institution", required = True)

def md5(filename):
    md5 = hashlib.md5()
    with open(filename, "rb") as stream:
        for chunk in iter(lambda: stream.read(64 * 1024), b''):
            md5.update(chunk)
    return md5.hexdigest()

class MyTardis:
    headers = { "accept": "application/json", "content-type": "application/json" }

    def __init__(self, base, username, password):
        self.base = base
        self.auth = (username, password)

    def url(self, obj):
        return "%s/api/v1/%s/" % (self.base, obj)

    def create(self, obj, data):
        return requests.post(self.url(obj), headers = self.headers, auth = self.auth, data = json.dumps(data))

    def location(self, response):
        if (response.status_code >= 201) and (response.status_code < 300):
            location = response.headers["location"]
            return location[location.index("/api"):]
        else:
            return None

    def create_experiment(self, title, description, institution):
        metadata = {
            "description": description,
            "institution_name": institution,
            "title": title
        }

        return self.location(self.create("experiment", metadata))

    def create_dataset(self, experiment, description):
        metadata = {
            "description": description,
            "experiments": [experiment],
            "immutable": False
        }

        return self.location(self.create("dataset", metadata))

    def create_file(self, dataset, name, size, md5, mime_type):
        metadata = {
            "dataset": dataset,
            "filename": name,
            "md5sum": md5,
            "size": size,
            "mimetype": mime_type,
            "replicas": [{
                "url": md5,
                "location": "default",
                "protocol": "file"
            }]
        }

        return self.location(self.create("dataset_file", metadata))

class HCP:
    def __init__(self, host, base, access, secret, bucket):
        self.base = base

        access = base64.b64encode(access)
        secret = hashlib.md5(secret).hexdigest()

        hs3 = S3Connection(aws_access_key_id = access, aws_secret_access_key = secret, host = host)
        self.bucket = hs3.get_bucket(bucket)

    def exists(self, obj):
        return self.bucket.get_key(self.base + obj) is not None

    def upload(self, filename):
        key = self.bucket.new_key(self.base + md5(filename))
        key.set_contents_from_filename(filename)

def sync_data(dir, hcp):
    objects = {}
    for subdir in glob.iglob("%s/*" % dir):
        if os.path.isdir(subdir):
            for file in glob.iglob("%s/*" % subdir):
                if os.path.isfile(file):
                    obj = md5(file)
                    objects[file] = obj
                    if not hcp.exists(obj):
                        hcp.upload(file)
    return objects

def upload_metadata(args, objects, mytardis):
    experiment = mytardis.create_experiment(args.title, args.description, args.institution)
    print experiment
    for subdir in glob.iglob("%s/*" % args.dir):
        if os.path.isdir(subdir):
            dataset_name = subdir.split("/")[-1]
            dataset = mytardis.create_dataset(experiment, dataset_name)
            print dataset
            for file in glob.iglob("%s/*" % subdir):
                if os.path.isfile(file):
                    print mytardis.create_file(dataset, file.split("/")[-1], os.stat(file).st_size, objects[file], "application/octet-stream")

def process_dir(args, mytardis, hcp):
    objects = sync_data(args.dir, hcp)
    upload_metadata(args, objects, mytardis)

def main():
    args = parser.parse_args()

    config = yaml.load(open("config.yaml", "r"))

    mytardis = MyTardis(config["mytardis"]["base"], config["mytardis"]["username"], config["mytardis"]["password"])
    hcp = HCP(config["hcp"]["host"], config["hcp"]["base"], config["hcp"]["access"], config["hcp"]["secret"], config["hcp"]["bucket"])

    process_dir(args, mytardis, hcp)

if __name__ == "__main__":
    main()
