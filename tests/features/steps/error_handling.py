"""
Behave steps for error_handling.feature

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import utils

MAX_WAIT_TIME = 70
SLEEP_TIME = 0.5


@given("that we have a directory structure with a corrupt json file")
def step_impl(context):
    context.tmpdir = Path(tempfile.mkdtemp())
    collection_dir = context.tmpdir / "db" / "collection"
    collection_dir.mkdir(parents=True)
    corrupt_file = collection_dir / "corrupt.json"
    context.corrupt_file = corrupt_file
    data = "{'val': 123}"
    with corrupt_file.open("w") as fp:
        fp.write(data)


@given("that the MongoDB server is down")
def step_impl(context):
    context.container.stop()


@when("we run mongolyin.py on that directory")
def step_impl(context):
    text = context.text.format(
        inputdir=context.tmpdir,
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
            shutil.copyfile(
                context.corrupt_file,
                context.tmpdir / "db" / "collection" / "data1.json",
            )

            time.sleep(1)

        finally:
            process.terminate()

        stderr.seek(0)
        context.mongolyin_logs = stderr.read()


@when("we run mongolyin.py while the server is down and copy files into the directory")
def step_impl(context):
    text = context.text.format(
        address=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    args = list(re.split(r"\s+", text))
    context.inputdir = Path(args[0])
    tmpdirname = tempfile.mkdtemp()
    context.tmpdir = Path(tmpdirname)
    fp, tmpfile = tempfile.mkstemp()
    context.tmpfile = Path(tmpfile)
    os.close(fp)
    args[0] = tmpdirname
    args = ["python", "-m", "mongolyin.mongolyin"] + args
    with context.tmpfile.open("w+") as stderr:
        process = subprocess.Popen(args, stderr=stderr)
        context.process = process
        utils.wait_for_ready(stderr)
        context.mongo_dbname = "db"
        context.mongo_collection = "collection"
        collection_dir = context.tmpdir / context.mongo_dbname / context.mongo_collection
        collection_dir.mkdir(parents=True)
        shutil.copytree(context.inputdir, collection_dir, dirs_exist_ok=True)


@then("it should log the error and continue without crashing")
def step_impl(context):
    error_text = context.text.strip()
    assert error_text in context.mongolyin_logs
    logs = context.mongolyin_logs.split("\n")
    logs = [l for l in logs if l.strip()]
    assert "Checking for new events..." in logs[-1]


@then("it should log the error")
def step_impl(context):
    tick = time.time()
    while time.time() - tick < MAX_WAIT_TIME:
        with context.tmpfile.open() as fp:
            logs = fp.read()

        if "pymongo.errors.ServerSelectionTimeoutError" in logs:
            return

        time.sleep(SLEEP_TIME)

    assert False


@then("upload the files when the server is back up")
def step_impl(context):
    utils.start_mongodb(context, password=context.mongo_password)
    tick = time.time()
    while time.time() - tick < MAX_WAIT_TIME:
        with context.tmpfile.open() as fp:
            logs = fp.read()

        if all([f"'data{i}.json' inserted into database with id" in logs for i in range(1, 6)]):
            return

        time.sleep(SLEEP_TIME)

    assert False
