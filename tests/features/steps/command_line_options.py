"""
Behave steps for upload_json.feature

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
import gridfs
from behave import *

import utils


@given("we have defined our MongoDB username and password in environment variables")
def step_impl(context):
    environ = os.environ.copy()
    environ["MONGODB_USERNAME"] = context.mongo_username
    environ["MONGODB_PASSWORD"] = context.mongo_password
    context.environ = environ


@given("we have defined our MongoDB server address in an environment variable")
def step_impl(context):
    environ = os.environ.copy()
    environ["MONGODB_ADDRESS"] = context.mongo_address
    context.environ = environ


@given("we have defined our MongoDB auth db in an environment variable")
def step_impl(context):
    environ = os.environ.copy()
    environ["MONGO_AUTH_DB"] = context.text.strip()
    context.environ = environ


@when("we run mongolyin.py without --address")
def step_impl(context):
    with tempfile.TemporaryDirectory() as tmpdirname:
        text = context.text.format(
            inputdir=tmpdirname,
            username=context.mongo_username,
            password=context.mongo_password,
        )
        args = list(re.split(r"\s+", text))
        args = ["python", "-m", "mongolyin.mongolyin"] + args
        with tempfile.TemporaryFile("w+") as stderr:
            try:
                process = subprocess.Popen(args, stderr=stderr, env=context.environ)
                utils.wait_for_ready(stderr)
                tmpdir = Path(tmpdirname)
                context.mongo_dbname = "db"
                context.mongo_collection = "collection"
                collection_dir = tmpdir / context.mongo_dbname / context.mongo_collection
                collection_dir.mkdir(parents=True)
                testfile = collection_dir / "data1.txt"
                context.testfile_name = testfile.name
                context.test_data = "123abc\n"
                with testfile.open("w") as fp:
                    fp.write(context.test_data)

                utils.wait_for_upload(stderr, [testfile.name])

            finally:
                process.terminate()


@when("we run mongolyin.py without --auth-db")
def step_impl(context):
    with tempfile.TemporaryDirectory() as tmpdirname:
        text = context.text.format(
            inputdir=tmpdirname,
            username=context.mongo_username,
            password=context.mongo_password,
            address=context.mongo_address,
        )
        args = list(re.split(r"\s+", text))
        args = ["python", "-m", "mongolyin.mongolyin"] + args
        with tempfile.TemporaryFile("w+") as stderr:
            try:
                process = subprocess.Popen(args, stderr=stderr, env=context.environ)
                utils.wait_for_ready(stderr)
                tmpdir = Path(tmpdirname)
                context.mongo_dbname = "db"
                context.mongo_collection = "collection"
                collection_dir = tmpdir / context.mongo_dbname / context.mongo_collection
                collection_dir.mkdir(parents=True)
                testfile = collection_dir / "data1.txt"
                context.testfile_name = testfile.name
                context.test_data = "123abc\n"
                with testfile.open("w") as fp:
                    fp.write(context.test_data)

                utils.wait_for_upload(stderr, [testfile.name])

            finally:
                process.terminate()


@when("we run monogolyin with --db")
def step_impl(context):
    context.mongo_dbname = "altdb"
    with tempfile.TemporaryDirectory() as tmpdirname:
        text = context.text.format(
            inputdir=tmpdirname,
            username=context.mongo_username,
            password=context.mongo_password,
            address=context.mongo_address,
            db=context.mongo_dbname,
        )
        args = list(re.split(r"\s+", text))
        args = ["python", "-m", "mongolyin.mongolyin"] + args
        with tempfile.TemporaryFile("w+") as stderr:
            try:
                process = subprocess.Popen(args, stderr=stderr)
                utils.wait_for_ready(stderr)
                tmpdir = Path(tmpdirname)
                context.mongo_collection = "collection"
                collection_dir = tmpdir / "db" / context.mongo_collection
                collection_dir.mkdir(parents=True)
                testfile = collection_dir / "data1.json"
                context.testfile_name = testfile.name
                context.test_data = {"val": 1}
                with testfile.open("w") as fp:
                    json.dump(context.test_data, fp)

                utils.wait_for_upload(stderr, [testfile.name])

            finally:
                process.terminate()


@when("we run monogolyin with --collection")
def step_impl(context):
    context.mongo_collection = "altcollection"
    with tempfile.TemporaryDirectory() as tmpdirname:
        text = context.text.format(
            inputdir=tmpdirname,
            username=context.mongo_username,
            password=context.mongo_password,
            address=context.mongo_address,
            collection=context.mongo_collection,
        )
        args = list(re.split(r"\s+", text))
        args = ["python", "-m", "mongolyin.mongolyin"] + args
        with tempfile.TemporaryFile("w+") as stderr:
            try:
                process = subprocess.Popen(args, stderr=stderr)
                utils.wait_for_ready(stderr)
                tmpdir = Path(tmpdirname)
                context.mongo_dbname = "db"
                collection_dir = tmpdir / context.mongo_dbname / "collection"
                collection_dir.mkdir(parents=True)
                testfile = collection_dir / "data1.json"
                context.testfile_name = testfile.name
                context.test_data = {"val": 1}
                with testfile.open("w") as fp:
                    json.dump(context.test_data, fp)

                utils.wait_for_upload(stderr, [testfile.name])

            finally:
                process.terminate()


@when("we run mongolyin.py without --username or --password")
def step_impl(context):
    with tempfile.TemporaryDirectory() as tmpdirname:
        text = context.text.format(inputdir=tmpdirname, address=context.mongo_address)
        args = list(re.split(r"\s+", text))
        args = ["python", "-m", "mongolyin.mongolyin"] + args
        with tempfile.TemporaryFile("w+") as stderr:
            try:
                process = subprocess.Popen(args, stderr=stderr, env=context.environ)
                utils.wait_for_ready(stderr)
                tmpdir = Path(tmpdirname)
                context.mongo_dbname = "db"
                context.mongo_collection = "collection"
                collection_dir = tmpdir / context.mongo_dbname / context.mongo_collection
                collection_dir.mkdir(parents=True)
                testfile = collection_dir / "data1.txt"
                context.testfile_name = testfile.name
                context.test_data = "123abc\n"
                with testfile.open("w") as fp:
                    fp.write(context.test_data)

                utils.wait_for_upload(stderr, [testfile.name])

            finally:
                process.terminate()


@then("it should correctly authenticate with the server")
def step_impl(context):
    kwargs = dict(
        host=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    with pymongo.MongoClient(**kwargs) as client:
        db = client[context.mongo_dbname]
        fs = gridfs.GridFS(db)
        results = list(fs.find({"filename": context.testfile_name}))
        assert len(results) == 1
        assert results[0].read().decode() == context.test_data


@then("it should upload files to the correct location on the server")
def step_impl(context):
    kwargs = dict(
        host=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    with pymongo.MongoClient(**kwargs) as client:
        db = client[context.mongo_dbname]
        collection = db[context.mongo_collection]
        doc = context.test_data.copy()
        doc["metadata.filename"] = context.testfile_name
        results = list(collection.find(doc))
        assert len(results) == 1
