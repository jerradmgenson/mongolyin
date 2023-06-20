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
import os
import subprocess
from pathlib import Path

import pymongo

import utils

MAX_WAIT_TIME = 70
SLEEP_TIME = 0.5


@when("we run mongolyin.py on a file with no collection directory")
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
                db_dir.mkdir()
                json_file = db_dir / "data.json"
                data = {"val": 1}
                with json_file.open("w") as fp:
                    json.dump(data, fp)

                utils.wait_for_upload(stderr, ["data.json"])

            finally:
                process.terminate()


@then("it should upload the file into the default collection")
def step_impl(context):
    collection_name = context.text.strip()
    kwargs = dict(
        host=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    with pymongo.MongoClient(**kwargs) as client:
        db = client[context.mongo_dbname]
        collection = db[collection_name]
        assert len(list(collection.find({"metadata.filename": "data.json"}))) == 1


@when("we run mongolyin.py on a file with no database directory")
def step_impl(context):
    tmpdirname = tempfile.mkdtemp()
    context.tmpdir = Path(tmpdirname)
    fp, tmpfile = tempfile.mkstemp()
    context.tmpfile = Path(tmpfile)
    os.close(fp)
    text = context.text.format(
        inputdir=tmpdirname,
        address=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    args = list(re.split(r"\s+", text))
    args = ["python", "-m", "mongolyin.mongolyin"] + args
    with context.tmpfile.open("w+") as stderr:
        process = subprocess.Popen(args, stderr=stderr)
        context.process = process
        utils.wait_for_ready(stderr)
        json_file = context.tmpdir / "data.json"
        data = {"val": 1}
        with json_file.open("w") as fp:
            json.dump(data, fp)


@then("it should ignore the file")
def step_impl(context):
    tick = time.time()
    while time.time() - tick < MAX_WAIT_TIME:
        time.sleep(SLEEP_TIME)
        with context.tmpfile.open() as fp:
            logs = fp.read()

        if all([re.search(r"File added: .+data\.json", logs),
                re.search(r"Not uploading .+data\.json because it isn't in a database directory", logs),
                "inserted into database with id" not in logs]):
            return

    assert False
