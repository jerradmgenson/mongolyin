"""
Behave steps for upload_spreadsheet.feature

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import os
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

import pandas as pd
import pymongo
from behave import *


@given("we have existing csv data in the database")
def step_impl(context):
    kwargs = dict(
        host=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    with pymongo.MongoClient(**kwargs) as client:
        db = client["db"]
        collection = db["collection"]
        context.filename = "data1.csv"
        test_data = [
            {"ID": 1, "Name": "Apple", "Quantity": 10, "Price": 0.5},
            {"ID": 2, "Name": "Orange", "Quantity": 15, "Price": 0.4},
        ]

        context.test_data = test_data
        test_data = deepcopy(test_data)
        for doc in test_data:
            metadata = dict(
                hash=sha256(str(doc).encode()).hexdigest(),
                filename=context.filename,
            )
            doc["metadata"] = metadata

        collection.insert_many(test_data)


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
                    if filepath.stem == "data6":
                        df = pd.read_csv(filepath, sep=";")

                    elif filepath.stem == "data7":
                        df = pd.read_csv(filepath, sep=";", decimal=",")

                    else:
                        df = pd.read_csv(filepath)

                else:
                    df = pd.read_excel(filepath)

                for col in df.columns:
                    if df[col].dtype == "object":
                        try:
                            df[col] = pd.to_numeric(df[col], errors="raise")

                        except ValueError:
                            pass

                for record in df.to_dict(orient="records"):
                    for key, value in record.items():
                        if pd.isnull(value):
                            record[key] = None

                    assert len(list(collection.find(record))) == 1


@then("it should upload csv data for the modified file")
def step_impl(context):
    kwargs = dict(
        host=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    with pymongo.MongoClient(**kwargs) as client:
        db = client[context.mongo_dbname]
        collection = db[context.mongo_collection]
        results = list(
            collection.find(
                {"metadata.filename": "data1.csv"},
                {"ID": 1, "Name": 1, "Quantity": 1, "Price": 1, "_id": 0},
            )
        )

        assert len(results) == 5
        test_data = {tuple(t.items()) for t in context.test_data}
        results = {tuple(r.items()) for r in results}
        assert results > test_data
