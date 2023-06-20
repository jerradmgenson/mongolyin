"""
Behave steps for error_handling.feature

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import json
import tempfile
import time
import re
import subprocess
from pathlib import Path

import pymongo

import utils

MAX_WAIT_TIME = 70
SLEEP_TIME = 0.5


@when("we run mongolyin.py on a path with multiple collection directories")
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
                db_dir = tmpdir / context.mongo_dbname
                collection_dir1 = db_dir / "collection1"
                collection_dir1.mkdir(parents=True)
                collection_dir2 = db_dir / "collection2"
                collection_dir2.mkdir(parents=True)
                json_file1 = db_dir / collection_dir1 / "data1.json"
                data = {"val": 1}
                with json_file1.open("w") as fp:
                    json.dump(data, fp)

                json_file2 = db_dir / collection_dir2 / "data2.json"
                with json_file2.open("w") as fp:
                    json.dump(data, fp)

                utils.wait_for_upload(stderr, ["data1.json", "data2.json"])

            finally:
                process.terminate()


@then("it should upload files from both directories")
def step_impl(context):
    kwargs = dict(
        host=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    with pymongo.MongoClient(**kwargs) as client:
        db = client[context.mongo_dbname]
        collection = db["collection1"]
        assert len(list(collection.find({"metadata.filename": "data1.json"}))) == 1
        collection = db["collection2"]
        assert len(list(collection.find({"metadata.filename": "data2.json"}))) == 1
