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

Copyright 2021 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""


import argparse
import copy
import json
import logging
import sys
import time
from functools import partial
from pathlib import Path

import bonobo
import gridfs
import pandas as pd
import pymongo
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

DEFAULT_COLLECTION_NAME = "_mongolyin_"
SPREADSHEET_EXTENSIONS = ".xls", ".xlsx", ".ods"
PANDAS_EXTENSIONS = SPREADSHEET_EXTENSIONS + (".csv", ".parquet")


def main(argv):
    """
    Main function for the program, handling the core logic.

    Args:
        argv: The command-line arguments.

    Returns:
        0, a standard exit status, which means the program exited without errors.

    """

    clargs = parse_command_line(argv)
    configure_logging(clargs.loglevel)
    mongo_client = MongoDBClient(
        clargs.address,
        clargs.username,
        clargs.password,
        clargs.auth_db,
        clargs.db,
        clargs.collection,
    )
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
    parser.add_argument("address", help="IP address or URL of the MongoDB server.")
    parser.add_argument("username", help="Username to use to authenticate with the MongoDB server.")
    parser.add_argument("password", help="Password to use to authenticate with the MongoDB server.")
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

    try:
        while True:
            time.sleep(ingest_frequency)

    except KeyboardInterrupt:
        observer.stop()

    observer.join()


def create_dispatch(mongo_client, ingress_path):
    def extract_pandas(filepath):
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
        with filepath.open() as fp:
            return json.load(fp)

    def extract_bin(filepath):
        with filepath.open("rb") as fp:
            return fp.read()

    def dispatch(filepath):
        """
        Creates a dispatch function which is capable of processing filepaths and
        handling them according to their file types.

        Args:
            mongo_client (MongoDBClient): An instance of MongoDBClient to perform database operations.
            ingress_path (str): The base directory which files are being ingested from.

        Returns:
            dispatch (func): A function capable of processing and uploading file data to MongoDB.

        """

        def get_filepaths():
            yield filepath

        logger = logging.getLogger(__name__)
        db_name = get_db_name(ingress_path, filepath)
        if not db_name:
            logger.debug(f"Not uploading {filepath} because it isn't in a database directory.")
            return

        tmp_client = mongo_client.with_db(db_name)
        collection_name = get_collection_name(ingress_path, filepath)
        tmp_client = tmp_client.with_collection(collection_name)
        graph = bonobo.Graph()
        if filepath.suffix in PANDAS_EXTENSIONS:
            graph.add_chain(get_filepaths, extract_pandas, tmp_client.insert_documents)

        elif filepath.suffix == ".json":
            graph.add_chain(get_filepaths, extract_json, tmp_client.insert_document)

        else:
            insert_file = partial(tmp_client.insert_file, filename=filepath.name)
            graph.add_chain(get_filepaths, extract_bin, insert_file)

        bonobo.run(graph)

    return dispatch


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
    if len(partial_path) >= 1:
        return partial_path[0]


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
    if len(partial_path) >= 2:
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

    """

    ingress_dir = ingress_path.name
    filepath_parts = list(filepath.parts)
    for part in filepath.parts:
        filepath_parts.pop(0)
        if part == ingress_dir:
            break

    return filepath_parts


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
            self._dispatch(event.src_path)

    def on_created(self, event):
        if not event.is_directory:
            logger = logging.getLogger(__name__)
            logger.info("File added: %s", event.src_path)
            self._dispatch(event.src_path)


class MongoDBClient:
    """
    A MongoDB client class which abstracts away the connection, insertion and other operations
    related to MongoDB. It provides the capability to handle single documents, multiple documents,
    and binary file insertion to the MongoDB database.

    Args:
        address (str): The IP address or URL of the MongoDB server.
        username (str): The username used to authenticate with the MongoDB server.
        password (str): The password used to authenticate with the MongoDB server.
        auth_db (str): The name of the MongoDB authentication database to use.
        db (str): The name of the database to write file data to.
        collection (str): The name of the collection to write file data to.
        client (MongoClient, optional): An existing MongoDB client object.

    """

    def __init__(self, address, username, password, auth_db, db, collection, client=None):
        self._address = address
        self._username = username
        self._password = password
        self._auth_db = auth_db
        self._db_name = db
        self._collection_name = collection
        self._client = client

    @property
    def client(self):
        if self._client is None:
            self._client = pymongo.MongoClient(
                self._address,
                username=self._username,
                password=self._password,
                authSource=self._auth_db,
            )
        return self._client

    @property
    def db(self):
        return self.client[self._db_name]

    @property
    def collection(self):
        return self.db[self._collection_name]

    @property
    def fs(self):
        return gridfs.GridFS(self.db)

    def insert_document(self, document, retry=1):
        """
        Insert a single document into the MongoDB collection.

        Args:
            document (dict): The document to be inserted.
            retry (int, optional): Number of times to retry in case of failure.

        """

        logger = logging.getLogger(__name__)
        if retry < 0:
            logger.error("Exceeded maximum number of retries. Failed to insert file.")

        try:
            self.collection.insert_one(document)

        except pymongo.errors.AutoReconnect as ar:
            logger.exception(ar)
            self._client = None
            self.insert_document(document, retry=retry - 1)

        except pymongo.errors.OperationFailure as of:
            logger.exception(of)
            self.insert_document(document, retry=retry - 1)

    def insert_documents(self, documents, retry=1):
        """
        Insert multiple documents into the MongoDB collection.

        Args:
            documents (list): A list of documents to be inserted.
            retry (int, optional): Number of times to retry in case of failure.

        """

        logger = logging.getLogger(__name__)
        if retry < 0:
            logger.error("Exceeded maximum number of retries. Failed to insert file.")

        try:
            self.collection.insert_many(documents)

        except pymongo.errors.AutoReconnect as ar:
            logger.exception(ar)
            self._client = None
            self.insert_documents(documents, retry=retry - 1)

        except pymongo.errors.OperationFailure as of:
            logger.exception(of)
            self.insert_documents(documents, retry=retry - 1)

    def insert_file(self, data, filename, retry=1):
        """
        Insert a binary file into the MongoDB GridFS.

        Args:
            data (bytes): Binary data of the file to be inserted.
            filename (str): The filename associated with the data.
            retry (int, optional): Number of times to retry in case of failure.

        """

        logger = logging.getLogger(__name__)
        if retry < 0:
            logger.error("Exceeded maximum number of retries. Failed to insert file.")

        try:
            self.fs.put(data, filename=filename)

        except pymongo.errors.AutoReconnect as ar:
            logger.exception(ar)
            self._client = None
            self.insert_file(data, filename, retry=retry - 1)

        except pymongo.errors.OperationFailure as of:
            logger.exception(of)
            self.insert_file(data, filename, retry=retry - 1)

    def with_db(self, db_name):
        """
        Return a new instance of MongoDBClient with the specified database name.

        Args:
            db_name (str): The name of the database to be used.

        Returns:
            MongoDBClient: A new instance of MongoDBClient with the specified database name.

        """

        if not self._db_name:
            new_client = copy.copy(self)
            new_client._db_name = db_name
            return new_client

        return self

    def with_collection(self, collection_name):
        """
        Return a new instance of MongoDBClient with the specified collection name.

        Args:
            collection_name (str): The name of the collection to be used.

        Returns:
            MongoDBClient: A new instance of MongoDBClient with the specified collection name.

        """

        if not self._collection_name:
            new_client = copy.copy(self)
            new_client._collection_name = collection_name
            return new_client

        return self


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
