"""
Behave steps for upload_spreadsheet.feature

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import os
import pandas as pd
from pathlib import Path

import pymongo
from behave import *


@then("it should upload the spreadsheet data into MongoDB")
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
                if filepath.suffix == ".csv":
                    df = pd.read_csv(filepath)

                else:
                    df = pd.read_excel(filepath)

                for record in df.to_dict(orient="records"):
                    assert len(list(collection.find(record))) == 1
