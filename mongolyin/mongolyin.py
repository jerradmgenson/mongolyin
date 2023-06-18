"""
Monitor a directory for new or modified files and upload them to a
MongoDB database. The target directory must have the following
structure:
- root_dir: files placed here will be ignored.
- root_dir/database: files placed here will be inserted into a default
                     collection in a database with the same name as the
                     immediate directory.
- root_dir/database/collection: files placed here will be inserted into
                                a collection with the same name as the
                                immediate directory and a database with
                                the same name as the parent of the
                                immediate directory.

No directory names after the third level directory are used. Files there
will still be ingested, but they will be treated as though they were
placed immediately under the 'collection' directory.

mongolyin can currently handle the following file types:
- CSV
- XLS
- XLSX
- ODS
- Parquet
- JSON

Files not in this list will still be ingested, but they will be treated
as binary files and inserted using GridFS.

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""


import argparse
import json
import logging
import os
import sys
import time
from functools import partial
from pathlib import Path

import bonobo
import pandas as pd
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from mongolyin.mongodbclient import MongoDBClient

DEFAULT_COLLECTION_NAME = "misc"
SPREADSHEET_EXTENSIONS = ".xls", ".xlsx", ".ods"
PANDAS_EXTENSIONS = SPREADSHEET_EXTENSIONS + (".csv", ".parquet")


def main(argv):
    """
    Main function for the program, handling the core logic.

    Args:
        argv: The command-line arguments.

    Returns:
        0, a standard exit status, which means the program exited without errors.
        1, indicates that one or more arguments are missing.

    """

    clargs = parse_command_line(argv)
    configure_logging(clargs.loglevel)
    logger = logging.getLogger(__name__)
    logger.debug("mongolyin started")
    username = clargs.username if clargs.username else os.environ.get("MONGODB_USERNAME")
    if not username:
        print("You must supply --username or define `MONGODB_USERNAME` in the environment.")
        return 1

    password = clargs.password if clargs.password else os.environ.get("MONGODB_PASSWORD")
    if not password:
        print("You must supply --password or define `MONGODB_PASSWORD` in the environment.")
        return 1

    auth_db = clargs.auth_db if clargs.auth_db else os.environ.get("MONGO_AUTH_DB")
    if not auth_db:
        print("You must supply --auth-db or define `MONGODB_AUTH_DB` in the environment.")
        return 1

    mongodb_client_args = (
        clargs.address,
        username,
        password,
        auth_db,
        clargs.db,
        clargs.collection,
    )

    with MongoDBClient(*mongodb_client_args) as mongo_client:
        dispatch = create_dispatch(mongo_client, clargs.ingress_path)
        event_handler = FileChangeHandler(dispatch)
        watch_directory(clargs.ingress_path, clargs.ingest_frequency, event_handler)

    return 0


def parse_command_line(argv):
    """
    Parses the command line arguments.

    Args:
        argv: A list of command line arguments.

    Returns:
        A Namespace object resulting from ArgumentParser.parse_args()

    """

    parser = argparse.ArgumentParser(
        prog="mongolyin",
        description="Ingest files from a directory into MongoDB.",
    )

    parser.add_argument("ingress_path", type=Path, help="Path to the ingress directory.")
    parser.add_argument("--address", help="IP address or URL of the MongoDB server.")
    parser.add_argument(
        "--username", help="Username to use to authenticate with the MongoDB server."
    )
    parser.add_argument(
        "--password", help="Password to use to authenticate with the MongoDB server."
    )
    parser.add_argument(
        "--auth-db", default="admin", help="Name of the MongoDB authentication database to use."
    )
    parser.add_argument(
        "--db",
        help="Name of the database to write file data to. If this isn't given, directory names are used instead.",
    )
    parser.add_argument(
        "--collection",
        help="Name of the collection to write file data. If this isn't given, subdirectory names are used instead.",
    )
    parser.add_argument(
        "--frequency",
        dest="ingest_frequency",
        default=60,
        type=float,
        help="How often to scan the ingest directory, in seconds.",
    )
    parser.add_argument(
        "--loglevel",
        default="info",
        help="Level to use for logging messages.",
    )

    return parser.parse_args(args=argv)


def configure_logging(loglevel):
    """
    Configures the logging module with the appropriate log level.

    Args:
        loglevel: String indicating the log level to be used.

    """

    intlevel = getattr(logging, loglevel.upper())
    root_logger = logging.getLogger()
    root_logger.setLevel(intlevel)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(intlevel)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)


def watch_directory(path, ingest_frequency, event_handler):
    """
    Monitors the directory for changes and triggers event handler when changes occur.

    Args:
        path: The path to the directory to monitor.
        ingest_frequency: The frequency at which to check for changes, in seconds.
        event_handler: The event handler to use with the Observer instance.

    """

    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    logger = logging.getLogger(__name__)
    logger.debug("mongolyin ready")
    try:
        while True:
            time.sleep(ingest_frequency)

    except KeyboardInterrupt:
        observer.stop()

    observer.join()


def create_dispatch(mongo_client, ingress_path, create_graph=bonobo.Graph, run_graph=bonobo.run):
    """
    Creates a dispatch function which handles the ingestion of different
    file types.

    Args:
        mongo_client (MongoDBClient): An instance of MongoDBClient to
                                      perform database operations.
        ingress_path (str): The base directory which files are being
                            ingested from.
        create_graph (func, optional): A function to create a new bonobo
                                       graph. Defaults to bonobo.Graph.
        run_graph (func, optional): A function to execute a bonobo graph.
                                    Defaults to bonobo.run.

    Returns:
        dispatch (func): A function capable of processing and uploading
                         file data to MongoDB.
    """

    def dispatch(filepath):
        def get_filepaths():
            yield filepath

        new_client = update_mongodb_client(mongo_client, ingress_path, filepath)
        if new_client is None:
            return

        extract, load = select_etl_functions(filepath, new_client)
        graph = create_graph()
        graph.add_chain(get_filepaths, file_ready_check, extract, load)
        run_graph(graph)

    return dispatch


def select_etl_functions(filepath, mongo_client):
    """
    Selects appropriate extraction and loading functions based on the
    file type.

    Args:
        filepath (Path): Path to the file.
        mongo_client (MongoDBClient): An instance of MongoDBClient to
                                      perform database operations.

    Returns:
        extract (func), load (func): The extraction and load functions
                                     selected for this file type.
    """

    if filepath.suffix in PANDAS_EXTENSIONS:
        extract = extract_pandas
        load = partial(mongo_client.insert_documents, filename=filepath.name)

    elif filepath.suffix == ".json":
        extract = extract_json
        load = partial(mongo_client.insert_document, filename=filepath.name)

    else:
        extract = extract_bin
        load = partial(mongo_client.insert_file, filename=filepath.name)

    return extract, load


def update_mongodb_client(mongo_client, ingress_path, filepath):
    """
    Updates the database and collection on the MongoDB client based on
    the file path.

    Args:
        mongo_client (MongoDBClient): An instance of MongoDBClient to
                                      perform database operations.
        ingress_path (str): The base directory which files are being
                            ingested from.
        filepath (Path): Path to the file.

    Returns:
        MongoDBClient: A new MongoDB client with the new database
                       and collection.

    """

    logger = logging.getLogger(__name__)
    db_name = get_db_name(ingress_path, filepath)
    if not db_name:
        logger.debug("Not uploading %s because it isn't in a database directory.", filepath)
        return None

    mongo_client = mongo_client.with_db(db_name)
    collection_name = get_collection_name(ingress_path, filepath)
    mongo_client = mongo_client.with_collection(collection_name)

    return mongo_client


def extract_pandas(filepath):
    """
    Extracts data from a pandas-compatible file into a list of
    dictionary records.

    Args:
        filepath (Path): Path to the file.

    Returns:
        List[Dict]: A list of dictionaries representing the data in the
                    file.

    Raises:
        ValueError: If the file type is not compatible with pandas.

    """

    if filepath.suffix == ".csv":
        df = pd.read_csv(filepath)

    elif filepath.suffix == ".parquet":
        df = pd.read_parquet(filepath)

    elif filepath.suffix in SPREADSHEET_EXTENSIONS:
        df = pd.read_excel(filepath)

    else:
        raise ValueError(f"This extractor can not read '{filepath.suffix} files.'")

    return df.to_dict(orient="records")


def extract_json(filepath):
    """
    Extracts data from a JSON file into a Python data structure.

    Args:
        filepath (Path): Path to the file.

    Returns:
        Any: The Python data structure obtained from the JSON file.

    """

    with filepath.open() as fp:
        return json.load(fp)


def extract_bin(filepath):
    """
    Extracts data from a binary file.

    Args:
        filepath (Path): Path to the file.

    Returns:
        bytes: The data read from the binary file.

    """

    with filepath.open("rb") as fp:
        return fp.read()


def get_db_name(ingress_path, filepath):
    """
    Determines the database name from the given file path, assuming the file is
    inside the ingress directory. The database name corresponds to the first directory
    inside the ingress directory.

    Args:
        ingress_path (str): The base directory which files are being ingested from.
        filepath (str): The full path of the file being processed.

    Returns:
        str: The name of the database, if the file is inside the ingress directory.
             Returns None otherwise.

    """

    partial_path = sub_path(ingress_path, filepath)
    # There should be at least one directory and a file in partial_path.
    if len(partial_path) >= 2:
        return partial_path[0]

    return None


def get_collection_name(ingress_path, filepath):
    """
    Determines the collection name from the given file path, assuming the file is
    inside the ingress directory. The collection name corresponds to the second directory
    inside the ingress directory.

    Args:
        ingress_path (str): The base directory which files are being ingested from.
        filepath (str): The full path of the file being processed.

    Returns:
        str: The name of the collection, if the file is inside the ingress directory
             and has a parent directory. If it doesn't have a parent directory,
             it returns the default collection name.

    """

    partial_path = sub_path(ingress_path, filepath)
    # There should be at least two directories and a file in partial_path.
    if len(partial_path) >= 3:
        return partial_path[1]

    return DEFAULT_COLLECTION_NAME


def sub_path(ingress_path, filepath):
    """
    Determines the relative path of the file from the ingress directory.

    Args:
        ingress_path (str): The base directory which files are being ingested from.
        filepath (str): The full path of the file being processed.

    Returns:
        list: A list of path parts starting from the first directory inside the
              ingress directory to the file.

    Raises:
      ValueError: when filepath is not within ingress_path.

    """

    ingress_dir = ingress_path.name
    filepath_parts = list(filepath.parts)
    for part in filepath.parts:
        filepath_parts.pop(0)
        if part == ingress_dir:
            return filepath_parts

    raise ValueError(f"filepath '{filepath}' not within ingress path '{ingress_path}'")


def file_ready_check(filepath, interval=0.2, timeout=5):
    """
    Block until the file is no longer being written to.

    Args:
      filepath: Path to the file to check.
      interval: Interval to wait between checking file size.
      timeout: Total time to block before raising TimeoutError.

    Returns:
      `filepath` when writing is finished.

    Raises:
      TimeoutError

    """

    tick = time.time()
    while time.time() - tick < timeout:
        try:
            size_before = os.path.getsize(filepath)
            time.sleep(interval)
            size_after = os.path.getsize(filepath)
            if size_before == size_after:
                return filepath

        except OSError:
            pass

    raise TimeoutError(f"Timed out while waiting to read '{filepath}'")


class FileChangeHandler(FileSystemEventHandler):
    """
    Class that inherits from FileSystemEventHandler and is responsible for handling
    file change events.

    """

    def __init__(self, dispatch):
        self._dispatch = dispatch
        super().__init__()

    def on_modified(self, event):
        if not event.is_directory:
            logger = logging.getLogger(__name__)
            logger.info("File modified: %s", event.src_path)
            self._dispatch(Path(event.src_path))

    def on_created(self, event):
        if not event.is_directory:
            logger = logging.getLogger(__name__)
            logger.info("File added: %s", event.src_path)
            self._dispatch(Path(event.src_path))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
