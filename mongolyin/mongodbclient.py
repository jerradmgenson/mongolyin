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
from hashlib import sha256
from typing import List, Optional

import gridfs
import pymongo

SLEEP_TIME = 30


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
        self._client = None
        self._connect()

    def _connect(self):
        self._client = pymongo.MongoClient(
            self._address,
            username=self._username,
            password=self._password,
            authSource=self._auth_db,
        )

    @property
    def db(self):
        return self._client[self._db_name]

    @property
    def collection(self):
        return self.db[self._collection_name]

    @property
    def fs(self):
        return gridfs.GridFS(self.db)

    def insert_document(self, document: dict, filename: str, retry: int = 1) -> Optional[str]:
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
        if retry < 0:
            logger.error("Exceeded maximum number of retries. Failed to insert '%s'", filename)
            return

        document_hash = sha256(str(document).encode()).hexdigest()
        if "metadata" not in document:
            document["metadata"] = {}

        document["metadata"]["hash"] = document_hash
        document["metadata"]["filename"] = filename
        document["metadata"]["date"] = datetime.datetime.now(datetime.timezone.utc)
        try:
            docs = self.collection.find({"metadata.filename": {"$eq": filename}}, {"metadata": 1})
            for doc in docs:
                try:
                    if doc["metadata"]["hash"] == document["metadata"]["hash"]:
                        logger.info("'%s' already exists in database, skipping", filename)
                        return

                except (KeyError, TypeError) as e:
                    logger.exception(e)

            insert_result = self.collection.insert_one(document)
            logger.info("'%s' inserted into database", filename)
            return insert_result.inserted_id

        except pymongo.errors.AutoReconnect as ar:
            logger.exception(ar)
            if retry > 0:
                time.sleep(SLEEP_TIME)
                self._connect()

            return self.insert_document(document, filename, retry=retry - 1)

        except pymongo.errors.OperationFailure as of:
            logger.exception(of)
            if retry > 0:
                time.sleep(SLEEP_TIME)

            return self.insert_document(document, filename, retry=retry - 1)

    def insert_documents(
        self, documents: List[dict], filename: str, retry: int = 1
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
        if retry < 0:
            logger.error(
                "Exceeded maximum number of retries. Failed to insert documents from '%s'", filename
            )
            return

        new_documents = []
        existing_hashes = set()

        # Fetch all documents with that filename and only the metadata field
        existing_documents = self.collection.find({"metadata.filename": filename}, {"metadata": 1})
        for doc in existing_documents:
            try:
                existing_hashes.add(doc["metadata"]["hash"])

            except (KeyError, TypeError) as e:
                logger.exception(e)

        # Prepare documents with metadata and check against existing hashes
        for document in documents:
            if "metadata" not in document:
                document["metadata"] = {}

            document["metadata"]["filename"] = filename
            document["metadata"]["hash"] = sha256(str(document).encode()).hexdigest()
            document["metadata"]["date"] = datetime.datetime.now(datetime.timezone.utc)

            if document["metadata"]["hash"] not in existing_hashes:
                new_documents.append(document)

            else:
                logger.info("Document already exists in database, skipping")

        if new_documents:
            try:
                insert_result = self.collection.insert_many(new_documents)
                logger.info(
                    "%d new documents inserted into database from '%s'",
                    len(new_documents),
                    filename,
                )

                return insert_result.inserted_ids

            except pymongo.errors.AutoReconnect as ar:
                logger.exception(ar)
                if retry > 0:
                    time.sleep(SLEEP_TIME)
                    self._connect()

                return self.insert_documents(documents, filename, retry=retry - 1)

            except pymongo.errors.OperationFailure as of:
                logger.exception(of)
                if retry > 0:
                    time.sleep(SLEEP_TIME)

                return self.insert_documents(documents, filename, retry=retry - 1)

        else:
            logger.info("No new documents to insert from '%s'", filename)

    def insert_file(self, data: bytes, filename: str, retry: int = 1) -> Optional[str]:
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
        if retry < 0:
            logger.error("Exceeded maximum number of retries. Failed to insert '%s'", filename)
            return

        try:
            files = self.fs.find({"filename": filename})
            for file_ in files:
                try:
                    if file_["metadata"]["hash"] == data_hash:
                        logger.info("'%s' already exists in database, skipping", filename)
                        return

                except (KeyError, TypeError) as e:
                    logger.exception(e)

            metadata = {
                "date": datetime.datetime.now(datetime.timezone.utc),
                "hash": data_hash,
            }

            result = self.fs.put(data, filename=filename, metadata=metadata)
            logger.info("'%s' inserted into database", filename)
            return result.inserted_id

        except pymongo.errors.AutoReconnect as ar:
            logger.exception(ar)
            if retry > 0:
                time.sleep(SLEEP_TIME)
                self._connect()

            return self.insert_file(data, filename, retry=retry - 1)

        except pymongo.errors.OperationFailure as of:
            logger.exception(of)
            if retry > 0:
                time.sleep(SLEEP_TIME)

            return self.insert_file(data, filename, retry=retry - 1)

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
