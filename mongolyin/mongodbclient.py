"""
Contains the implementation of the MongoDBClient class.

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import copy
import datetime
import gc
import logging
from functools import singledispatch, singledispatchmethod, wraps
from hashlib import sha256
from typing import Dict, Generator, List, Optional

import gridfs
import pandas as pd
import pymongo


@singledispatch
def convert_strings_to_numbers(data):
    """
    Converts string columns to numeric where possible in tabular data.

    """

    raise TypeError(f"convert_strings_to_numbers does not take arguments of type '{type(data)}'")


@convert_strings_to_numbers.register
def convert_dataframe_strings_to_numbers(df: pd.DataFrame):
    """
    Converts string columns to numeric where possible in a DataFrame.

    This function iterates over each column in a DataFrame. If the column
    type is 'object' (Pandas' internal type for string), it tries to convert
    the column to a numeric type. If any value in the column cannot be
    converted to a numeric type, the function leaves the column as strings.
    In addition, it replaces commas with periods before attempting the
    conversion.

    Args:
        df (pd.DataFrame): The DataFrame whose string columns are to be
                           converted to numeric where possible.

    Returns:
        pd.DataFrame: The DataFrame with string columns converted to numeric
                      where possible.

    """

    for col in df.columns:
        if df[col].dtype == "object":  # if the column is a string
            try:
                # Replace commas with periods and attempt conversion to numeric
                df[col] = pd.to_numeric(df[col].str.replace(",", "."), errors="raise")

            except ValueError:
                pass  # If any value raises a ValueError when converting, leave the column as strings
    return df


@convert_strings_to_numbers.register(list)
def convert_dict_strings_to_numbers(docs: List[dict]):
    """
    Convert columns of string values in a list of dictionaries to numbers if possible.

    Args:
      docs: list of dict
        The list of dictionaries to be converted. Each dictionary represents a row of data,
        and each key-value pair in the dictionary corresponds to a column and its value in that row.

    Returns:
      list of dict
        The converted list of dictionaries. Dictionaries are directly modified in the input list.

    """

    convert_columns = {k: None for k, v in docs[0].items() if isinstance(v, str)}
    for doc in docs:
        for col, convert in convert_columns.copy().items():
            try:
                new_val = doc[col].replace(",", ".")
                if convert is None or convert == int:
                    int(new_val)
                    convert_columns[col] = int

                else:
                    float(new_val)

            except ValueError:
                try:
                    float(new_val)
                    convert_columns[col] = float

                except ValueError:
                    del convert_columns[col]

            except AttributeError:
                del convert_columns[col]

    for doc in docs:
        for col, convert in convert_columns.items():
            doc[col] = convert(doc[col].replace(",", "."))

    return docs


def disconnect_on_error(func):
    """
    A decorator that disconnects a pymongo client when an error occurs, and re-raises the exception.

    This function is intended to be used as a decorator for methods of a
    class that maintain an active pymongo client connection (stored in
    `self._client`). When a decorated method raises either a `pymongo.errors.AutoReconnect`
    or `pymongo.errors.OperationFailure` exception, the decorator catches
    the exception, disconnects the client by calling `self.close()`, logs the error,
    and then re-raises the exception.

    Args:
        func (Callable): The function to be decorated. It is expected to be a
                         method of a class that contains `self._client` and
                         `self.close()` for managing a pymongo client connection.

    Returns:
        Callable: The decorated function which disconnects the client upon
                  encountering specified pymongo errors and re-raises the exception.
    """

    @wraps(func)
    def wrapped_func(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)

        except (pymongo.errors.AutoReconnect, pymongo.errors.OperationFailure) as e:
            if self._client:
                self.close()

            logger = logging.getLogger(__name__)
            logger.debug("Closed pymongo client due to error: %s", str(e))
            raise

    return wrapped_func


def gridfs_fallback(func):
    """
    A decorator that provides a fallback mechanism for document insertion
    using GridFS when the document size exceeds MongoDB's limit.

    This function wraps around another function that performs insertion
    of a document into MongoDB. If the document size exceeds the maximum
    BSON document size, the function catches the
    `pymongo.errors.DocumentTooLarge` exception, logs a warning message,
    and inserts the document using GridFS instead.

    Args:
        func (Callable): The function to be decorated. This function
                         should take as arguments a document to be
                         inserted into MongoDB and a filename, and should
                         return an identifier for the inserted document.

    Returns:
        Callable: The decorated function which provides a fallback mechanism
                  to insert a document using GridFS when the document size
                  exceeds MongoDB's limit.

    Wrapped Function Args:
        document (Dict or List[Dict]): The document(s) to be inserted into
                                       MongoDB. A document can be a single
                                       Python dictionary or a list of
                                       dictionaries.
        filename (str): The filename associated with the document.

    Wrapped Function Returns:
        str or List[str]: The ObjectID(s) generated for the inserted
                          document(s) in MongoDB. It returns the ObjectID
                          when a single document is inserted and a list
                          of ObjectIDs when multiple documents are inserted.
                          In case of failure, the wrapped function returns None.
    """

    @wraps(func)
    def wrapped_func(self, document, filename, *args, **kwargs):
        try:
            return func(self, document, filename, *args, **kwargs)

        except pymongo.errors.DocumentTooLarge:
            logger = logging.getLogger(__name__)
            logger.warning("'%s' exceeds max document size. Inserting with GridFS", filename)
            return self.insert_file(str(document).encode(), filename)

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
        buffer_size (int): Maximum number of documents to read from a file for each insertion.
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
        buffer_size: int,
        client: pymongo.MongoClient = None,
    ):
        self._address = address
        self._username = username
        self._password = password
        self._auth_db = auth_db
        self._db_name = db
        self._collection_name = collection
        self._buffer_size = buffer_size
        self._client = client

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
            if self._client:
                self._client.close()

        except (pymongo.errors.ConnectionFailure, pymongo.errors.AutoReconnect) as e:
            logger = logging.getLogger(__name__)
            logger.exception(e)

        finally:
            self._client = None

    @property
    def client(self):
        if self._client is None:
            self._connect()

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

    @singledispatchmethod
    @disconnect_on_error
    @gridfs_fallback
    def insert_document(
        self, document: Dict, filename: str, existing_documents=None
    ) -> Optional[str]:
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
        logger.info(
            "'%s' inserted into database with id '%s'",
            filename,
            insert_result.inserted_id,
        )
        return insert_result.inserted_id

    @insert_document.register(list)
    @disconnect_on_error
    @gridfs_fallback
    def _(
        self, documents: List[Dict], filename: str, existing_documents=None
    ) -> Optional[List[str]]:
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
        if not existing_documents:
            existing_documents = self._get_existing_documents(filename)

        # Prepare documents with metadata and check against existing hashes
        for document in documents:
            sorted_items = str(sorted(list(document.items())))
            if sorted_items in existing_documents:
                logger.info("Document already exists in database, skipping")
                continue

            if "metadata" not in document:
                document["metadata"] = {}

            document["metadata"]["filename"] = filename
            document["metadata"]["date"] = datetime.datetime.now(datetime.timezone.utc)
            new_documents.append(document)

        if new_documents:
            insert_result = self.collection.insert_many(new_documents)
            logger.info(
                "%d new documents inserted into database from '%s'",
                len(new_documents),
                filename,
            )

            logger.debug("'%s' inserted into database", filename)
            return insert_result.inserted_ids

        logger.info("No new documents to insert from '%s'", filename)
        return None

    @disconnect_on_error
    def insert_generator(self, data: Generator, filename: str) -> Optional[List[str]]:
        """
        Insert documents from a generator object into the MongoDB collection.

        The method chunks the generator data based on the set buffer size
        and then passes these chunks for insertion.

        Args:
            data (Generator): A generator object that yields the documents to be inserted.
            filename (str): The filename associated with the documents.

        Returns:
            A list of ObjectIDs generated for the inserted documents, or None if the
            insertion fails.

        """

        inserted_ids = []
        data_buffer = []
        existing_documents = self._get_existing_documents(filename)
        for d in data:
            data_buffer.append(d)
            if self._buffer_size != -1 and len(data_buffer) >= self._buffer_size:
                inserted_ids.extend(
                    self._insert_generator(data_buffer, filename, existing_documents)
                )
                data_buffer = []
                gc.collect()

        if data_buffer:
            inserted_ids.extend(self._insert_generator(data_buffer, filename, existing_documents))

        return inserted_ids

    def _insert_generator(self, data, filename, existing_documents):
        """
        Convert a chunk of generator data to pandas DataFrame, preprocess the data,
        and then insert into the MongoDB collection.

        Args:
            data (list): A list of documents to be inserted.
            filename (str): The filename associated with the documents.

        Returns:
            A list of ObjectIDs generated for the inserted documents, or None if the
            insertion fails.

        """

        data = [dict(d) for d in data]
        data = convert_strings_to_numbers(data)
        return self.insert_document(data, filename, existing_documents=existing_documents)

    def _get_existing_documents(self, filename: str):
        """
        Fetches and returns a set of existing documents with a given
        filename in the MongoDB collection.

        Args:
            filename (str): The filename to look for in the 'metadata.filename' field.

        Returns:
            set: A set of existing documents in string format with the
                 given filename. Each document in the set is a string
                 representation of sorted list of items in the document.

        """

        existing_documents = set()

        # Fetch all documents with that filename and only the metadata field
        query_results = self.collection.find(
            {"metadata.filename": filename}, {"metadata": 0, "_id": 0}
        )

        for doc in query_results:
            sorted_items = str(sorted(list(doc.items())))
            existing_documents.add(sorted_items)

        return existing_documents

    @disconnect_on_error
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
        logger.info("'%s' inserted into database with id '%s'", filename, result)
        return result

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
