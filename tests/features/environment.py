"""
Behave test environment configuration.

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import time
import subprocess
import string
import random

import docker
import pymongo
from pymongo.errors import ServerSelectionTimeoutError


def before_all(context):
    client = docker.from_env()
    try:
        client.images.get("mongo_testdb")

    except docker.errors.ImageNotFound:
        print("Test MongoDB image not found, building...")
        subprocess.check_call(["docker", "build", "-f", "Dockerfile.testdb", "-t", "mongo_testdb", "."])

    context.docker = docker.DockerClient.from_env()
    context.mongo_username = "root"
    context.mongo_password = ''.join(random.choices(string.ascii_letters + string.digits, k=20))
    context.mongo_address = "mongodb://localhost:27018"
    context.container = context.docker.containers.run(
        "mongo_testdb",
        detach=True,
        ports={'27017/tcp': 27018},
        remove=True,
        environment={
            "MONGO_INITDB_ROOT_USERNAME": context.mongo_username,
            "MONGO_INITDB_ROOT_PASSWORD": context.mongo_password
        },
    )

    wait_for_mongo(context.mongo_address)


def after_all(context):
    context.container.stop()


def wait_for_mongo(db_uri, timeout=30):
    client = pymongo.MongoClient(db_uri, serverSelectionTimeoutMS=timeout)
    start_time = time.time()
    while True:
        try:
            # The ismaster command is cheap and does not require auth.
            client.admin.command('ismaster')
            break
        except ServerSelectionTimeoutError:
            if time.time() - start_time > timeout:
                raise TimeoutError("MongoDB did not initialize within the expected time.")
            time.sleep(0.1)
