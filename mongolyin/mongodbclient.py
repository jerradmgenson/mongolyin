"""
Contains the implementation of the MongoDBClient class.

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import copy
import datetime
import logging
import time
from functools import wraps
from hashlib import sha256
from typing import List, Optional

import gridfs
import pymongo

SLEEP_TIME = 30


def with_retry(func):
    """
    Add a keyword argument 'retry' to 'func' that causes 'func' to be retried
    'retry' times upon encountering a 'pymongo.errors.AutoReconnect' or
    'pymongo.errors.OperationFailure' exception. Also log exception if/when one
    occurs and call 'self._connect()' when an 'AutoReconnect' exception occurs
    to attempt to reconnect manually.

    Args:
      func: The function to wrap.

    Returns:
      The wrapped function.

    Raises:
      MaxRetriesExceeded

    """

    @wraps(func)
    def wrapped_func(self, *args, retry=1, **kwargs):
        logger = logging.getLogger(__name__)
        if retry < 0:
            raise MaxRetriesExceeded("Exceeded maximum number of retries")

        try:
            return func(self, *args, **kwargs)

        except pymongo.errors.AutoReconnect as ar:
            logger.exception(ar)
            if retry > 0:
                time.sleep(SLEEP_TIME)
                self._connect()

            return wrapped_func(self, *args, retry=retry - 1, **kwargs)

        except pymongo.errors.OperationFailure as of:
            logger.exception(of)
            if retry > 0:
                time.sleep(SLEEP_TIME)

            return wrapped_func(self, *args, retry=retry - 1, **kwargs)

    return wrapped_func


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

    def __init__(
        self,
        address: str,
        username: str,
        password: str,
        auth_db: str,
        db: str,
        collection: str,
        client: pymongo.MongoClient = None,
    ):
        self._address = address
        self._username = username
        self._password = password
        self._auth_db = auth_db
        self._db_name = db
        self._collection_name = collection
        self._client = client
        if self._client is None:
            self._connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()
        if exc_type is not None:
            return False

        return True

    def _connect(self):
        if self._client:
            self.close()

        self._client = pymongo.MongoClient(
            self._address,
            username=self._username,
            password=self._password,
            authSource=self._auth_db,
        )

    def close(self):
        """
        Close the connection to the database. If errors occur, log them
        and continue execution.

        """

        try:
            self._client.close()

        except (pymongo.errors.ConnectionFailure, pymongo.errors.AutoReconnect) as e:
            logger = logging.getLogger(__name__)
            logger.exception(e)

    @property
    def db(self):
        return self._client[self._db_name]

    @property
    def collection(self):
        return self.db[self._collection_name]

    @property
    def fs(self):
        return gridfs.GridFS(self.db)

    @with_retry
    def insert_document(self, document: dict, filename: str) -> Optional[str]:
        """
        Insert a single document into the MongoDB collection.

        Args:
            document (dict): The document to be inserted.
            filename (str): Name of the source file.
            retry (int, optional): Number of times to retry in case of failure.

        Returns:
            The ObjectID generated for the inserted document, or None if the
            insertion fails.

        """

        logger = logging.getLogger(__name__)
        document_hash = sha256(str(document).encode()).hexdigest()
        if "metadata" not in document:
            document["metadata"] = {}

        document["metadata"]["hash"] = document_hash
        document["metadata"]["filename"] = filename
        document["metadata"]["date"] = datetime.datetime.now(datetime.timezone.utc)
        docs = self.collection.find({"metadata.filename": {"$eq": filename}}, {"metadata": 1})
        for doc in docs:
            with ExceptionLogger((KeyError, TypeError)):
                if doc["metadata"]["hash"] == document["metadata"]["hash"]:
                    logger.info("'%s' already exists in database, skipping", filename)
                    return None

        insert_result = self.collection.insert_one(document)
        logger.info("'%s' inserted into database", filename)
        return insert_result.inserted_id

    @with_retry
    def insert_documents(self, documents: List[dict], filename: str) -> Optional[List[str]]:
        """
        Insert multiple documents into the MongoDB collection.

        Args:
            documents (list): A list of documents to be inserted.
            filename (str): The filename associated with the documents.
            retry (int, optional): Number of times to retry in case of failure.

        Returns:
            The ObjectIDs generated for the inserted documents, or None if the
            insertion fails.

        """

        logger = logging.getLogger(__name__)
        new_documents = []
        existing_hashes = set()

        # Fetch all documents with that filename and only the metadata field
        existing_documents = self.collection.find({"metadata.filename": filename}, {"metadata": 1})
        for doc in existing_documents:
            with ExceptionLogger((KeyError, TypeError)):
                existing_hashes.add(doc["metadata"]["hash"])

        # Prepare documents with metadata and check against existing hashes
        for document in documents:
            document_hash = sha256(str(document).encode()).hexdigest()
            if "metadata" not in document:
                document["metadata"] = {}

            document["metadata"]["filename"] = filename
            document["metadata"]["hash"] = document_hash
            document["metadata"]["date"] = datetime.datetime.now(datetime.timezone.utc)

            if document["metadata"]["hash"] not in existing_hashes:
                new_documents.append(document)

            else:
                logger.info("Document already exists in database, skipping")

        if new_documents:
            insert_result = self.collection.insert_many(new_documents)
            logger.info(
                "%d new documents inserted into database from '%s'",
                len(new_documents),
                filename,
            )

            logger.info("'%s' inserted into database", filename)
            return insert_result.inserted_ids

        logger.info("No new documents to insert from '%s'", filename)
        return None

    @with_retry
    def insert_file(self, data: bytes, filename: str) -> Optional[str]:
        """
        Insert a binary file into the MongoDB GridFS.

        Args:
            data (bytes): Binary data of the file to be inserted.
            filename (str): The filename associated with the data.
            retry (int, optional): Number of times to retry in case of failure.

        Returns:
            The ObjectID generated for the inserted file, or None if the
            insertion fails.

        """

        logger = logging.getLogger(__name__)
        data_hash = sha256(data).hexdigest()
        files = list(self.fs.find({"filename": filename, "metadata.hash": data_hash}))
        if len(files) != 0:
            logger.info("'%s' already exists in database, skipping", filename)
            return None

        metadata = {
            "date": datetime.datetime.now(datetime.timezone.utc),
            "hash": data_hash,
        }

        result = self.fs.put(data, filename=filename, metadata=metadata)
        logger.info("'%s' inserted into database", filename)
        return result.inserted_id

    def with_db(self, db_name: str):
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

    def with_collection(self, collection_name: str):
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


class ExceptionLogger:
    """
    A context manager to catch, log and suppress specified exceptions.

    Args:
        exception_types (list): A list of exception classes to catch.

    Usage:
        with ExceptionLogger([ZeroDivisionError, ValueError]):
            # some code that might raise an exception
            1 / 0

    """

    def __init__(self, exception_types):
        self.exception_types = exception_types

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            if any(issubclass(exc_type, t) for t in self.exception_types):
                logger = logging.getLogger(__name__)
                logger.error("Exception occurred", exc_info=(exc_type, exc_val, exc_tb))
                return True  # suppress specified exceptions and continue execution

        return False  # do not suppress other exceptions


class MaxRetriesExceeded(Exception):
    """
    Raised when the maximum number of retries is exceeded.

    """
