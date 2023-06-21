"""
Behave steps for upload_bin.feature

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import os
from pathlib import Path

import gridfs
import pandas as pd
import pymongo
from behave import *


@given("we have existing binary data in the database")
def step_impl(context):
    kwargs = dict(
        host=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    with pymongo.MongoClient(**kwargs) as client:
        db = client["db"]
        fs = gridfs.GridFS(db)
        context.test_data = b"123456789qwertyuiop"
        context.filename = "data1.bin"
        context.metadata = {"hash": "testhash"}
        fs.put(context.test_data, filename=context.filename, metadata=context.metadata)


@then("it should upload the binary data into MongoDB")
def step_impl(context):
    kwargs = dict(
        host=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    with pymongo.MongoClient(**kwargs) as client:
        db = client[context.mongo_dbname]
        fs = gridfs.GridFS(db)
        for root, _, files in os.walk(context.inputdir):
            root = Path(root)
            for file_ in files:
                filepath = root / file_
                results = list(fs.find({"filename": file_}))
                assert len(results) == 1
                with filepath.open("rb") as fp:
                    file_contents = fp.read()

                assert file_contents == results[0].read()


@then("it should upload binary data for the modified file")
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
        assert len(results) == 2
        matches_test_data = False
        not_matches_test_data = False
        for result in results:
            if result.metadata["hash"] == context.metadata["hash"]:
                matches_test_data = True

            else:
                not_matches_test_data = True

        assert matches_test_data and not_matches_test_data
