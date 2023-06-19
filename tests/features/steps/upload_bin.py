"""
Behave steps for upload_bin.feature

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import os
import pandas as pd
from pathlib import Path

import pymongo
import gridfs
from behave import *


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
