"""
Behave steps for existing_files.feature

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import os
import re
import tempfile
import time
import shutil
import subprocess
import pandas as pd
from pathlib import Path

import pymongo
import gridfs
from behave import *

import utils


@given("we have a directory with preexisting files")
def step_impl(context):
    context.inputdir = Path(context.text.strip())
    assert context.inputdir.exists()


@when("we run mongolyin.py on the directory with preexisting files")
def step_impl(context):
    text = context.text.format(
        address=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    args = list(re.split(r"\s+", text))
    with tempfile.TemporaryDirectory() as tmpdirname:
        args[0] = tmpdirname
        args = ["python", "-m", "mongolyin.mongolyin"] + args
        with tempfile.TemporaryFile("w+") as stderr:
            context.mongo_dbname = "db"
            context.mongo_collection = "collection"
            collection_dir = Path(tmpdirname) / context.mongo_dbname / context.mongo_collection
            collection_dir.mkdir(parents=True)
            shutil.copytree(context.inputdir, collection_dir, dirs_exist_ok=True)
            try:
                process = subprocess.Popen(args, stderr=stderr)
                utils.wait_for_ready(stderr)
                time.sleep(5)

            finally:
                process.terminate()


@then("it should not upload the files")
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
                results = list(collection.find({"metadata.filename": file_}))
                assert len(results) == 0
