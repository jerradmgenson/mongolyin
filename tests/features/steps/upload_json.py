"""
Behave steps for upload_json.feature

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import gridfs
import pymongo
import utils
from behave import *
from bson import BSON


@given("we have existing json data in the database")
def step_impl(context):
    kwargs = dict(
        host=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    with pymongo.MongoClient(**kwargs) as client:
        db = client["db"]
        collection = db["collection"]
        test_data = {"testkey": "testval", "metadata": {}}
        test_data["metadata"]["filename"] = "data1.json"
        test_data["metadata"]["hash"] = "testhash"
        collection.insert_one(test_data)
        context.test_data = test_data


@when("we run mongolyin.py and copy files into the directory")
def step_impl(context):
    text = context.text.format(
        address=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    args = list(re.split(r"\s+", text))
    context.inputdir = Path(args[0])
    with tempfile.TemporaryDirectory() as tmpdirname:
        args[0] = tmpdirname
        args = ["python", "-m", "mongolyin.mongolyin"] + args
        with tempfile.TemporaryFile("w+") as stderr:
            try:
                process = subprocess.Popen(args, stderr=stderr)
                utils.wait_for_ready(stderr)
                tmpdir = Path(tmpdirname)
                context.mongo_dbname = "db"
                context.mongo_collection = "collection"
                collection_dir = tmpdir / context.mongo_dbname / context.mongo_collection
                collection_dir.mkdir(parents=True)
                shutil.copytree(context.inputdir, collection_dir, dirs_exist_ok=True)
                filenames = list(utils.read_filenames(context.inputdir))
                utils.wait_for_upload(stderr, filenames)

            finally:
                process.terminate()


@then("it should upload the json data into MongoDB")
def step_impl(context):
    kwargs = dict(
        host=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    with pymongo.MongoClient(**kwargs) as client:
        db = client[context.mongo_dbname]
        collection = db[context.mongo_collection]
        for root, _, files in os.walk(context.inputdir):
            root = Path(root)
            for file_ in files:
                filepath = root / file_
                with filepath.open() as fp:
                    file_data = json.load(fp)

                assert len(list(collection.find(file_data))) == 1


@then("it should upload json data for the modified file")
def step_impl(context):
    kwargs = dict(
        host=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    with pymongo.MongoClient(**kwargs) as client:
        db = client[context.mongo_dbname]
        collection = db[context.mongo_collection]
        results = list(collection.find({"metadata.filename": "data1.json"}))
        assert len(results) == 2
        matches_test_data = False
        not_matches_test_data = False
        for result in results:
            if result["metadata"]["hash"] == context.test_data["metadata"]["hash"]:
                matches_test_data = True

            else:
                not_matches_test_data = True

        assert matches_test_data and not_matches_test_data


@when("we run mongolyin.py and copy a large json file into the directory")
def step_impl(context):
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmpdir = Path(tmpdirname)
        text = context.text.format(
            inputdir=tmpdirname,
            address=context.mongo_address,
            username=context.mongo_username,
            password=context.mongo_password,
        )

        args = list(re.split(r"\s+", text))
        args = ["python", "-m", "mongolyin.mongolyin"] + args
        with tempfile.TemporaryFile("w+") as stderr:
            try:
                process = subprocess.Popen(args, stderr=stderr)
                utils.wait_for_ready(stderr)
                context.mongo_dbname = "db"
                context.mongo_collection = "collection"
                collection_dir = tmpdir / context.mongo_dbname / context.mongo_collection
                collection_dir.mkdir(parents=True)
                context.filename = "data.json"
                generate_large_json(collection_dir / context.filename)
                utils.wait_for_upload(stderr, [context.filename])

            finally:
                process.terminate()


def generate_large_json(path):
    data = {}
    bson_data = b""
    id = 1
    min_filesize = 17000000
    while len(bson_data) < min_filesize:
        for _ in range(10000):
            user_key = f"User{id}"
            user_data = {"id": id, "name": f"User{id}"}
            data[user_key] = user_data
            id += 1

        bson_data = BSON.encode(data)

    with path.open("w") as fp:
        json.dump(data, fp)


@then("it should upload the json data using GridFS")
def step_impl(context):
    kwargs = dict(
        host=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    with pymongo.MongoClient(**kwargs) as client:
        db = client[context.mongo_dbname]
        fs = gridfs.GridFS(db)
        results = list(fs.find({"filename": context.filename}))
        assert len(results) == 1
