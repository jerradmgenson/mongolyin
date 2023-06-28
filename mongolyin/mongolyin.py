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
import gc
import json
import logging
import os
import sys
import time
from collections import deque
from functools import partial
from pathlib import Path
from queue import Empty, Queue

import clevercsv
import ijson
import pandas as pd
import psutil
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from mongolyin import etl
from mongolyin.mongodbclient import MongoDBClient, convert_strings_to_numbers

DEFAULT_ADDRESS = "mongodb://localhost:27017"
DEFAULT_BUFFER_SIZE = 1000
DEFAULT_COLLECTION_NAME = "misc"
RESTART_SIZE = 157286400  # 150 MB
SPREADSHEET_EXTENSIONS = ".xls", ".xlsx", ".ods"
PANDAS_EXTENSIONS = SPREADSHEET_EXTENSIONS + (".parquet",)


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
        print(
            "You must supply --username or define `MONGODB_USERNAME` in the environment.",
            file=sys.stderr,
        )
        return 1

    password = clargs.password if clargs.password else os.environ.get("MONGODB_PASSWORD")
    if not password:
        print(
            "You must supply --password or define `MONGODB_PASSWORD` in the environment.",
            file=sys.stderr,
        )
        return 1

    auth_db = clargs.auth_db if clargs.auth_db else os.environ.get("MONGO_AUTH_DB", "admin")
    logger.info("Authentication database: %s", auth_db)
    if clargs.address:
        address = clargs.address

    else:
        address = os.environ.get("MONGODB_ADDRESS", DEFAULT_ADDRESS)

    if clargs.buffer_size:
        buffer_size = clargs.buffer_size

    else:
        buffer_size = int(os.environ.get("MONGOLYIN_BUFFER_SIZE", DEFAULT_BUFFER_SIZE))

    if buffer_size < 1 and buffer_size != -1:
        raise ValueError(f"buffer size must be greater than 1 or equal to -1, not '{buffer_size}'")

    logger.info("Server address: %s", address)
    mongodb_client_args = (
        address,
        username,
        password,
        auth_db,
        clargs.db,
        clargs.collection,
        buffer_size,
    )

    with MongoDBClient(*mongodb_client_args) as mongo_client:
        dispatch, process = create_dispatch(mongo_client, clargs.ingress_path)
        event_handler = FileChangeHandler(dispatch)
        watch_directory(clargs.ingress_path, event_handler, process, clargs.sleep_time)

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

    parser.add_argument("--auth-db", help="Name of the MongoDB authentication database to use.")

    parser.add_argument(
        "--db",
        help="Name of the database to write file data to. If this isn't given, directory names are used instead.",
    )

    parser.add_argument(
        "--collection",
        help="Name of the collection to write file data. If this isn't given, subdirectory names are used instead.",
    )

    parser.add_argument(
        "--loglevel",
        default="info",
        help="Level to use for logging messages.",
    )

    parser.add_argument(
        "--sleep-time",
        default=2,
        type=float,
        help="Time (in seconds) to sleep in-between checking for file changes.",
    )

    parser.add_argument(
        "--buffer-size",
        type=int,
        help="Maximum number of documents to read from a file for each insertion.",
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


def watch_directory(path, event_handler, process, sleep_time):
    """
    Monitors the directory for changes and triggers event handler when changes occur.

    Args:
        path: The path to the directory to monitor.
        event_handler: The event handler to use with the Observer instance.
        process: Function to call to process dispatch arguments.
        sleep_time: Time (in seconds) to sleep in-between checking for
                    new events.

    """

    observer = Observer(timeout=sleep_time)
    observer.schedule(event_handler, path, recursive=True)
    logger = logging.getLogger(__name__)
    proc = psutil.Process(os.getpid())
    try:
        observer.start()
        logger.debug("mongolyin ready")
        while True:
            logger.debug("Checking for new events...")
            new_events = True
            while new_events:
                new_events = process()

            if proc.memory_info().rss >= RESTART_SIZE:
                break

            time.sleep(sleep_time)

    except KeyboardInterrupt:
        pass

    finally:
        observer.stop()
        observer.join()


def create_dispatch(mongo_client, ingress_path, debounce_time=0.1):
    """
    Creates a dispatch function which handles the ingestion of different
    file types.

    Args:
        mongo_client (MongoDBClient): An instance of MongoDBClient to
                                      perform database operations.
        ingress_path (str): The base directory which files are being
                            ingested from.
        debounce_time (float, optional): Number of seconds that must pass after
                                         the last event arrives before we begin
                                         processing events.

    Returns:
        dispatch (func): A function capable of processing and uploading
                         file data to MongoDB.
        process (func): A function that is called to process arguments
                        given to `dispatch`.

    """

    event_queue = Queue()
    debounce_queue = SetQueue()

    def process():
        """
        Processes events from `dispatch()` by running them through an ETL pipeline.

        The function operates in a loop checking the event queue for new files.
        When a new file is detected, it is added to a debounce queue to be
        processed after a debounce time period.

        The debounce queue is a unique set of files waiting to be processed.
        It ensures the same file isn't processed multiple times if detected more
        than once within the debounce period.

        If an ETLException is raised during the 'file ready check' or 'load' stages
        of the pipeline, the filepath is added back into the debounce queue
        to be processed again later.

        If the event queue is empty and no new events have been detected within
        the debounce time, the function breaks out of the loop and returns False.

        Returns:
            bool: False if the event queue and debounce queue are empty and no
                  new events have been detected within the debounce time.
                  True otherwise, indicating a successful processing.

        """

        last_event_time = time.time()
        changes_detected = False
        while True:  # Debounce loop
            try:
                debounce_queue.push(event_queue.get_nowait())
                changes_detected = True
                last_event_time = time.time()

            except Empty:
                # event_queue is empty
                if not changes_detected:
                    # Break immediately if there's nothing in the queue
                    # and no new events are detected.
                    break

                else:
                    # If there was an initial event, continue looping until no
                    # new events arrive during the debounce period.
                    if time.time() - last_event_time >= debounce_time:
                        # Debounce period exceeded without new events.
                        break

                    elif (delay := debounce_time - (time.time() - last_event_time)) > 0:
                        # Still within debounce period - sleep and continue looping.
                        time.sleep(delay)

        if not debounce_queue:
            return False

        filepath = debounce_queue.pop()
        new_client = update_mongodb_client(mongo_client, ingress_path, filepath)
        if new_client is None:
            return True

        try:
            extract, load = select_etl_functions(filepath, new_client)

        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.exception(e)
            return True

        pipeline = etl.Pipeline(
            etl.Stage("file ready check", file_ready_check),
            etl.Stage("extract", extract),
            etl.Stage("load", load),
        )

        try:
            pipeline.run(filepath)

        except etl.ETLException as etle:
            if etle.stage_name in ("file ready check", "load"):
                debounce_queue.push(filepath)

            return False

        finally:
            del pipeline
            gc.collect()

        return True

    return event_queue.put, process


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
        load = partial(mongo_client.insert_document, filename=filepath.name)

    elif filepath.suffix == ".csv":
        extract = clevercsv.wrappers.stream_dicts
        load = partial(mongo_client.insert_generator, filename=filepath.name)

    elif filepath.suffix == ".json":
        if get_json_type(filepath) == "dict":
            extract = extract_json
            load = partial(mongo_client.insert_document, filename=filepath.name)

        else:
            extract = extract_json_chunks
            load = partial(mongo_client.insert_generator, filename=filepath.name)

    else:
        extract = extract_bin
        load = partial(mongo_client.insert_file, filename=filepath.name)

    return extract, load


def get_json_type(filepath):
    """
    Determines if the root of the JSON file is a list or a dictionary.

    This function reads the first non-whitespace character from a JSON file to determine
    if the root of the JSON structure is a list or a dictionary. If the first character
    is '[', it returns 'list'. If it's '{', it returns 'dict'. Otherwise, it raises a
    ValueError.

    Args:
        filename (Path): Path of the JSON file.

    Returns:
        str: 'list' if the root of the JSON file is a list, 'dict' if it's a dictionary.

    Raises:
        ValueError: If the JSON file does not start with '[' or '{'.
    """

    with filepath.open() as file:
        for chunk in iter(lambda: file.read(1), ""):
            for char in chunk:
                if not char.isspace():
                    if char == "[":
                        return "list"

                    elif char == "{":
                        return "dict"

                    else:
                        raise ValueError("JSON file does not start with '[' or '{'")

    raise ValueError(f"'{filepath}' is an empty file")


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

    if filepath.suffix == ".parquet":
        df = pd.read_parquet(filepath)

    elif filepath.suffix in SPREADSHEET_EXTENSIONS:
        df = pd.read_excel(filepath)

    else:
        raise ValueError(f"This extractor can not read '{filepath.suffix} files.'")

    records = df.to_dict(orient="records")

    # Convert np.nan values to None
    for record in records:
        for key, value in record.items():
            if pd.isnull(value):
                record[key] = None

    return convert_strings_to_numbers(records)


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


def extract_json_chunks(filepath):
    """
    Generator that yields individual JSON objects from a file.

    This function uses the ijson library to lazily parse a JSON file. The function expects
    the JSON file to have a list of objects as its root. It yields one object at a time,
    allowing the processing of large JSON files that do not fit into memory.

    Args:
        filepath (Path): The path to the JSON file.

    Yields:
        dict: The next JSON object in the file.

    """

    with filepath.open() as fp:
        objects = ijson.items(fp, "item")
        for row in objects:
            yield row


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


def file_ready_check(filepath, interval=0.1, timeout=5):
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


class SetQueue:
    """
    A combined set and queue (deque) data structure.

    Implements a deque where items are unique. Trying to add an item that already exists
    in the SetQueue is a no-op.

    Attributes:
        queue (deque): The queue that stores the items.
        set_ (set): The set that ensures the uniqueness of items.

    """

    def __init__(self):
        self._queue = deque()
        self._set = set()

    def push(self, item):
        """
        Push an item to the queue.

        If the item is already in the SetQueue, it doesn't do anything.

        Args:
            item: The item to be pushed to the queue.

        """

        if item not in self._set:
            self._queue.append(item)
            self._set.add(item)

    def pop(self):
        """
        Pop an item from the queue.

        Returns:
            The popped item.

        Raises:
            IndexError: If the SetQueue is empty.

        """

        item = self._queue.popleft()
        self._set.remove(item)
        return item

    def __contains__(self, item):
        return item in self._set

    def __len__(self):
        return len(self._queue)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
