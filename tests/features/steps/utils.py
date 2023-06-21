"""
Utility functions for behave test steps.

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import os
import random
import string
import subprocess
import time

import docker
import pymongo
from pymongo.errors import ServerSelectionTimeoutError


def wait_for_ready(logstream, timeout=10):
    """
    Block until mongolyin process to be ready to monitor directory changes.

    Args:
      logstream: A stream of log messages from mongolyin opened for reading.
      timeout (optional): Number of seconds to wait before timing out.

    Returns:
      True (always)

    Raises:
      TimeoutError: if the monoglyin process takes longer to be ready than the
                    time specified by `timeout`.

    """

    tick = time.time()
    while time.time() - tick < timeout:
        logstream.seek(0)
        if "mongolyin ready" in logstream.read():
            return True

        time.sleep(0.1)

    logstream.seek(0)
    logs = logstream.read()
    raise TimeoutError("Timed out waiting for mongolyin to be ready.\n\n" + logs)


def wait_for_upload(logstream, filenames, timeout=10):
    """
    Block until mongolyin process has uploaded all expected files.

    Args:
      logstream: A stream of log messages from mongolyin opened for reading.
      filenames: A sequence of filenames that are expected to be uploaded.
      timeout (optional): Number of seconds to wait before timing out.

    Returns:
      True (always)

    Raises:
      TimeoutError: if the monoglyin process takes longer to be ready than the
                    time specified by `timeout`.

    """

    tick = time.time()
    while time.time() - tick < timeout:
        logstream.seek(0)
        logs = logstream.read()
        if all([f"'{f}' inserted into database" in logs for f in filenames]):
            return True

        time.sleep(0.1)

    raise TimeoutError("Timed out waiting for mongolyin to upload files.\n\n" + logs)


def wait_for_mongo(db_uri, timeout=30):
    client = pymongo.MongoClient(db_uri, serverSelectionTimeoutMS=timeout)
    start_time = time.time()
    while True:
        try:
            # The ismaster command is cheap and does not require auth.
            client.admin.command("ismaster")
            break
        except ServerSelectionTimeoutError:
            if time.time() - start_time > timeout:
                raise TimeoutError("MongoDB did not initialize within the expected time.")
            time.sleep(0.1)


def read_filenames(directory):
    """
    Read filenames recursively from `directory`.

    Returns:
      A generator of filenames from `directory`.

    """

    for _, _, files in os.walk(directory):
        for file_ in files:
            yield file_

    client = docker.from_env()
    try:
        client.images.get("mongo_testdb")

    except docker.errors.ImageNotFound:
        print("Test MongoDB image not found, building...")
        subprocess.check_call(
            ["docker", "build", "-f", "Dockerfile.testdb", "-t", "mongo_testdb", "."]
        )


def start_mongodb(context, password=None):
    """
    Start a docker container running a MongoDB server.

    """

    context.docker = docker.DockerClient.from_env()
    context.mongo_username = "root"
    if password:
        context.mongo_password = password

    else:
        context.mongo_password = "".join(random.choices(string.ascii_letters + string.digits, k=20))

    context.mongo_address = "mongodb://localhost:27018"
    context.container = context.docker.containers.run(
        "mongo_testdb",
        detach=True,
        ports={"27017/tcp": 27018},
        remove=True,
        environment={
            "MONGO_INITDB_ROOT_USERNAME": context.mongo_username,
            "MONGO_INITDB_ROOT_PASSWORD": context.mongo_password,
        },
    )

    wait_for_mongo(context.mongo_address)
