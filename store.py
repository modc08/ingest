#!/usr/bin/env python

# pylint: disable=line-too-long

"""A basic tool for interacting with MyTardis + Hitachi Content Platform (which pretends to be S3)."""

import argparse, base64, glob, hashlib, json, os, shutil, sys
import requests, xlrd, yaml

from xlrd import open_workbook

from boto.s3.connection import S3Connection

parser = argparse.ArgumentParser(
    description="Upload and register an experiment.",
    epilog="Remember to populate config.yaml with appropriate settings.")
parser.add_argument("-D", "--dir", help="Directory", required=True)
parser.add_argument("-t", "--title", help="Title", required=True)
parser.add_argument("-a", "--authors", help="Authors (semi-colon separated, format \"Last, First\")", required=True)
parser.add_argument("-d", "--description", help="Description", required=True)
parser.add_argument("-f", "--force", help="Force overwrite of existing metadata", action="store_true")
parser.add_argument("-r", "--remove", help="Remove experiment directory after successful upload", action="store_true")

# Note: sheet processing order matters!
valid_sheets = ["organism", "analysis", "source", "sample", "extract", "library", "sequence", "processing"]

class MyTardis(object):
    headers = {"accept": "application/json", "content-type": "application/json"}

    def __init__(self, config):
        self.base = config["base"]
        self.auth = (config["username"], config["password"])
        self.institution = config["institution"]

    def url(self, obj, key=None):
        url_str = "%s/api/v1/%s/" % (self.base, obj)
        if key:
            return "%s%s/" % (url_str, key)
        else:
            return url_str

    @staticmethod
    def prefix(obj, key):
        return "/api/v1/%s/%s/" % (obj, key)

    def create(self, obj, data):
        response = requests.post(self.url(obj), headers=self.headers, auth=self.auth, data=json.dumps(data))
        if response.status_code == 201:
            return response
        else:
            raise Exception("HTTP error: %i\n\n%s" % (response.status_code, response.text))

    def fetch(self, obj, key):
        response = requests.get(self.url(obj, key), headers=self.headers, auth=self.auth)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception("HTTP error: %i\n\n%s" % (response.status_code, response.text))

    def exists(self, obj, key):
        response = requests.get(self.url(obj, key), headers=self.headers, auth=self.auth)
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        else:
            raise Exception("HTTP error: %i\n\n%s" % (response.status_code, response.text))

    @staticmethod
    def location(response):
        if (response.status_code >= 201) and (response.status_code < 300):
            location_header = response.headers["location"]
            return location_header[location_header.index("/api"):]
        else:
            raise Exception("HTTP error: %i\n\n%s" % (response.status_code, response.text))

    def create_experiment(self, title, description):
        metadata = {
            "description": description,
            "institution_name": self.institution,
            "title": title
        }

        return self.location(self.create("experiment", metadata))

    def create_dataset(self, experiment, description):
        metadata = {
            "description": description,
            "experiments": [experiment],
            "experiment": experiment,
            "immutable": False
        }

        return self.location(self.create("dataset", metadata))

    def create_file(self, dataset, name, size, md5_hex, mime_type):
        metadata = {
            "dataset": dataset,
            "filename": name,
            "md5sum": md5_hex,
            "size": size,
            "mimetype": mime_type,
            "replicas": [{
                "url": md5_hex,
                "location": "default",
                "protocol": "file"
            }]
        }

        return self.location(self.create("dataset_file", metadata))

    def create_author(self, experiment, author, order):
        metadata = {
            "author": author,
            "experiment": experiment,
            "order" : order
        }

        return self.location(self.create("authorexperiment", metadata))


class HCP(object):
    def __init__(self, config):
        self.base = config["base"]

        access = base64.b64encode(config["access"])
        secret = hashlib.md5(config["secret"]).hexdigest()

        hs3 = S3Connection(aws_access_key_id=access, aws_secret_access_key=secret, host=config["host"])
        self.bucket = hs3.get_bucket(config["bucket"])

    def exists(self, obj):
        return self.bucket.get_key(self.base + obj) is not None

    def upload(self, filename):
        key = self.bucket.new_key(self.base + md5(filename))
        key.set_contents_from_filename(filename)


def md5(filename):
    md5hash = hashlib.md5()
    with open(filename, "rb") as stream:
        for chunk in iter(lambda: stream.read(64 * 1024), b''):
            md5hash.update(chunk)
    return md5hash.hexdigest()


def sync_data(directory, hcp):
    print "Synchronising with the object store...",
    objects = {}
    for subdir in glob.iglob("%s/*" % directory):
        if os.path.isdir(subdir):
            for datafile in glob.iglob("%s/*" % subdir):
                if os.path.isfile(datafile):
                    obj = md5(datafile)
                    objects[datafile] = obj
                    if not hcp.exists(obj):
                        hcp.upload(datafile)
    print "done."
    return objects


def upload_metadata(args, objects, mytardis):
    experiment = mytardis.create_experiment(args.title, args.description)
    print "New experiment:", args.title
    for subdir in glob.iglob("%s/*" % args.dir):
        if os.path.isdir(subdir):
            dataset_name = subdir.split("/")[-1]
            dataset = mytardis.create_dataset(experiment, dataset_name)
            print "New dataset:", dataset_name
            for datafile in [f for f in glob.glob("%s/*" % subdir) if os.path.isfile(f)]:
                datafilename = datafile.split("/")[-1]
                if datafilename == "metadata.xlsx":
                    print "Processing:", datafile
                    sheets = {}
                    for sheet in open_workbook(file_contents=open(datafile, "rb").read()).sheets():
                        for name in sheet.name.lower().split():
                            if name in valid_sheets:
                                sheets[name] = sheet
                                break
                    process_metadata(sheets, mytardis, dataset, force=args.force)
                else:
                    mytardis.create_file(
                        dataset, datafilename, os.stat(datafile).st_size,
                        objects[datafile], "application/octet-stream")
                    print "New datafile:", datafilename
    return experiment


def load_cells(sheet):
    # skip hidden rows
    first_row = 0
    while len(str.join("", [str(cell.value) for cell in sheet.row(first_row)])) == 0:
        first_row += 1

    if first_row + 1 == sheet.nrows:
        return None

    # skip hidden columns
    first_col = 0
    while len(str.join("", [str(cell.value) for cell in sheet.col(first_col)])) == 0:
        first_col += 1

    data = []
    fields = [field.value for field in sheet.row(first_row)]
    for row in range(first_row+1, sheet.nrows-first_row):
        item = {}
        for col in range(first_col, sheet.ncols-first_col):
            cell_type = sheet.cell_type(row, col)
            if cell_type == xlrd.XL_CELL_DATE:
                date_tuple = xlrd.xldate_as_tuple(sheet.cell_value(row, col), sheet.book.datemode)
                item[fields[col]] = "%04i-%02i-%02i" % (date_tuple[0], date_tuple[1], date_tuple[2])
            elif cell_type == xlrd.XL_CELL_NUMBER:
                item[fields[col]] = int(sheet.cell_value(row, col))
            else:
                item[fields[col]] = sheet.cell_value(row, col)
        data.append(item)
    return data


def process_metadata(sheets, mytardis, dataset, force=False):
    for sheet_name in valid_sheets:
        if sheet_name in sheets:
            sheet_data = load_cells(sheets[sheet_name])

            if not sheet_data:
                continue

            for item in sheet_data:
                if "id" not in item:
                    raise Exception("Item does not have 'id' field: %s" % json.dumps(item))

                if sheet_name == "analysis":
                    item["dataset"] = dataset
                elif sheet_name == "source":
                    item["organism"] = mytardis.prefix("organism", item["organism"])
                elif sheet_name == "sample":
                    item["source"] = mytardis.prefix("source", item["source"])
                    item["organism"] = mytardis.prefix("organism", item["organism"])
                elif sheet_name == "extract":
                    item["sample"] = mytardis.prefix("sample", item["sample"])
                elif sheet_name == "library":
                    item["extract"] = mytardis.prefix("extract", item["extract"])
                elif sheet_name == "sequence":
                    item["library"] = mytardis.prefix("library", item["library"])
                elif sheet_name == "processing":
                    item["sequence"] = mytardis.prefix("sequence", item["sequence"])
                    item["analysis"] = mytardis.prefix("analysis", item["analysis"])

                if force or not mytardis.exists(sheet_name, item["id"]):
                    print "Uploading metadata: %s/%s" % (sheet_name, item["id"])
                    mytardis.create(sheet_name, strip_empty_values(item))
                else:
                    print "Skipping existing metadata: %s/%s" % (sheet_name, item["id"])


def strip_empty_values(data):
    stripped = {}
    for key in data:
        if data[key] != "":
            stripped[key] = data[key]
    return stripped


def process_dir(args, mytardis, hcp):
    objects = sync_data(args.dir, hcp)
    return upload_metadata(args, objects, mytardis)


def upload_authors(mytardis, experiment, authors):
    order = 0
    for author in [author.strip() for author in authors.split(";")]:
        print "Registering author: %s" % author
        mytardis.create_author(experiment, author, order)
        order += 1


def main():
    args = parser.parse_args()

    args.dir = args.dir.strip()

    if args.dir in ["/", ".", ""]:
        print "Error: please use the name of the experiment directory rather than \"%s\"." % args.dir
        sys.exit(1)

    config = yaml.load(open("config.yaml", "r"))

    mytardis = MyTardis(config["mytardis"])
    hcp = HCP(config["hcp"])

    experiment = process_dir(args, mytardis, hcp)

    upload_authors(mytardis, experiment, args.authors)

    if args.remove:
        print "Removing:", args.dir
        shutil.rmtree(args.dir)

    print "Done!"


if __name__ == "__main__":
    main()
