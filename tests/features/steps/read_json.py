"""
Behave steps for read_json feature.

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import os
import re
import json
import tempfile
import shutil
import subprocess
from pathlib import Path

import pymongo
from behave import *

import utils


@when("we run mongolyin.py with the correct arguments")
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
