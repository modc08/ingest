#!/usr/bin/env python

# pylint: disable=line-too-long,unused-argument

"""A basic tool for interacting with MyTardis + Hitachi Content Platform (which pretends to be S3)."""

import base64, datetime, glob, hashlib, json, os, sys
import boto, pytz, requests, xlrd

from xlrd import open_workbook

from boto.s3.connection import S3Connection

spreadsheet_filenames = ["metadata.xlsx", "metadata.xls"]

class MyTardis(object):
    headers = {"accept": "application/json", "content-type": "application/json"}

    # Note: sheet processing order matters!
    valid_sheets = ["organism", "analysis", "source", "sample", "extract", "library", "sequence", "processing"]

    def __init__(self, config):
        self.base = config["base"]
        self.auth = (config["username"], config["password"])
        self.verify = "//www." in self.base
        if not self.verify:
            requests.packages.urllib3.disable_warnings()

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
        response = requests.post(self.url(obj), headers=self.headers, auth=self.auth, data=json.dumps(data), verify=self.verify)
        if response.status_code == 201:
            return response
        else:
            raise Exception("HTTP error: %i\n\n%s" % (response.status_code, response.text))

    def fetch(self, obj, key=None):
        response = requests.get(self.url(obj, key), headers=self.headers, auth=self.auth, verify=self.verify)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception("HTTP error: %i\n\n%s" % (response.status_code, response.text))

    def exists(self, obj, key):
        response = requests.get(self.url(obj, key), headers=self.headers, auth=self.auth, verify=self.verify)
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

    def create_experiment(self, title, description, institution=None):
        metadata = {
            "description": description,
            "title": title
        }

        if institution:
            metadata["institution_name"] = institution

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

    def upload_authors(self, experiment, authors):
        order = 0
        for author in [author.strip() for author in authors.split(";")]:
            print "Registering author: %s" % author
            self.create_author(experiment, author, order)
            order += 1

    def upload_metadata(self, objects, directory, title, description, authors, institution=None, force=False, **kwargs):
        experiment = self.create_experiment(title, description, institution)
        print "New experiment:", title
        for subdir in glob.iglob("%s/*" % directory):
            if os.path.isdir(subdir):
                dataset_name = subdir.split("/")[-1]
                dataset = self.create_dataset(experiment, dataset_name)
                print "New dataset:", dataset_name
                for dirpath, dirnames, filenames in os.walk(subdir):
                    for datafile in [os.path.join(dirpath, filename) for filename in filenames]:
                        datafilename = datafile.split("/")[-1]
                        if datafilename in spreadsheet_filenames:
                            print "Processing:", datafile
                            sheets = {}
                            for sheet in open_workbook(file_contents=open(datafile, "rb").read()).sheets():
                                for name in sheet.name.lower().split():
                                    if name in self.valid_sheets:
                                        sheets[name] = sheet
                                        break
                            self.process_metadata(sheets, dataset, force)
                        else:
                            self.create_file(
                                dataset, datafilename, os.stat(datafile).st_size,
                                objects[datafile], "application/octet-stream")
                            print "New datafile:", datafilename
        self.upload_authors(experiment, authors)
        return experiment

    @staticmethod
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
                    val = float(sheet.cell_value(row, col))
                    if val.is_integer():
                        item[fields[col]] = int(val)
                    else:
                        item[fields[col]] = val
                else:
                    item[fields[col]] = sheet.cell_value(row, col).strip()
            data.append(item)
        return data

    @staticmethod
    def strip_empty_values(data):
        stripped = {}
        for key in data:
            if data[key] != "":
                stripped[key] = data[key]
        return stripped

    def process_metadata(self, sheets, dataset, force):
        for sheet_name in self.valid_sheets:
            if sheet_name in sheets:
                sheet_data = self.load_cells(sheets[sheet_name])

                if not sheet_data:
                    continue

                for item in sheet_data:
                    if "id" not in item:
                        raise Exception("Item does not have 'id' field: %s" % json.dumps(item))

                    if sheet_name == "analysis":
                        item["dataset"] = dataset
                    elif sheet_name == "source":
                        item["organism"] = self.prefix("organism", item["organism"])
                    elif sheet_name == "sample":
                        item["source"] = self.prefix("source", item["source"])
                        item["organism"] = self.prefix("organism", item["organism"])
                    elif sheet_name == "extract":
                        item["sample"] = self.prefix("sample", item["sample"])
                    elif sheet_name == "library":
                        item["extract"] = self.prefix("extract", item["extract"])
                    elif sheet_name == "sequence":
                        item["library"] = self.prefix("library", item["library"])
                    elif sheet_name == "processing":
                        item["sequence"] = self.prefix("sequence", item["sequence"])
                        item["analysis"] = self.prefix("analysis", item["analysis"])

                    if force or not self.exists(sheet_name, item["id"]):
                        print "Uploading metadata: %s/%s" % (sheet_name, item["id"])
                        self.create(sheet_name, self.strip_empty_values(item))
                    else:
                        print "Skipping existing metadata: %s/%s" % (sheet_name, item["id"])

class HCP(object):
    epoch = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)

    def __init__(self, config):
        self.base = config["base"]

        access = base64.b64encode(config["access"])
        secret = hashlib.md5(config["secret"]).hexdigest()

        hs3 = S3Connection(aws_access_key_id=access, aws_secret_access_key=secret, host=config["host"])
        self.bucket = hs3.get_bucket(config["bucket"])

    def exists(self, obj, add_prefix=True):
        if add_prefix:
            return self.bucket.get_key(self.base + obj) is not None
        else:
            return self.bucket.get_key(obj) is not None

    def upload(self, filename, key=None):
        if key:
            key = self.bucket.new_key(key)
        else:
            key = self.bucket.new_key(self.base + self.md5file(filename))
        if not self.exists(key, False):
            key.set_contents_from_filename(filename)
            return True
        else:
            return False

    def sync(self, directory):
        print "Synchronising with the object store..."
        sys.stdout.flush()
        objects = {}
        if not os.path.isdir(directory):
            raise ValueError("not a directory: %s" % directory)
        for subdir in glob.iglob("%s/*" % directory):
            if os.path.isdir(subdir):
                for dirpath, dirnames, filenames in os.walk(subdir):
                    for datafile in [os.path.join(dirpath, filename) for filename in filenames]:
                        basename = datafile.split("/")[-1]
                        if basename not in spreadsheet_filenames:
                            obj = self.md5file(datafile)
                            objects[datafile] = obj
                            if not self.exists(obj):
                                print "Uploading %s" % datafile
                                self.upload(datafile)
        return objects

    def list(self):
        objects = []
        for key in self.bucket:
            if key.size > 0:
                timestamp = pytz.utc.localize(boto.utils.parse_ts(key.last_modified))
                mtime = int((timestamp - self.epoch).total_seconds())
                objects.append({ "name" : key.name, "size" : key.size, "mtime" : mtime })
        return objects

    def delete(self, key):
        self.bucket.delete_key(key)

    @staticmethod
    def md5file(filename):
        md5hash = hashlib.md5()
        with open(filename, "rb") as stream:
            for chunk in iter(lambda: stream.read(64 * 1024), b''):
                md5hash.update(chunk)
        return md5hash.hexdigest()
