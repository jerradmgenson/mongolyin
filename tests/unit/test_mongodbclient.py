"""
Unit tests for mongodbclient.MongoDBClient

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import datetime
import logging
import unittest
from unittest.mock import MagicMock, patch

import gridfs
import pymongo

from mongolyin.mongodbclient import MongoDBClient


class TestMongoDBClient(unittest.TestCase):
    def setUp(self):
        self.patcher = patch.object(MongoDBClient, "_connect")
        self.mock_connect = self.patcher.start()
        self.client = MongoDBClient(
            address="test_address",
            username="test_user",
            password="test_pass",
            auth_db="test_auth_db",
            db="test_db",
            collection="test_collection",
        )

        self.client._client = MagicMock()

    def tearDown(self):
        self.patcher.stop()

    @patch.object(MongoDBClient, "collection")
    def test_insert_document_no_existing(self, mock_collection):
        document = {"test_key": "test_value"}

        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "a49fe606-c777-41e2-a3e1-2f63a7691e18"
        mock_collection.insert_one.return_value = mock_insert_result
        mock_collection.find.return_value = []

        result = self.client.insert_document(document, "test_filename")

        mock_collection.insert_one.assert_called_once_with(document)
        self.assertEqual(result, mock_insert_result.inserted_id)
        self.mock_connect.assert_called_once()
        mock_collection.find.assert_called_once_with(
            {"metadata.filename": {"$eq": "test_filename"}}, {"metadata": 1}
        )

    @patch.object(MongoDBClient, "collection")
    def test_insert_document_with_existing_match(self, mock_collection):
        document = {"test_key": "test_value"}
        metadata = {
            "filename": "test_filename",
            "date": datetime.datetime.now(),
            "hash": "d02670a74f98da498c8268aa1e6725f6241d68c5a6c1c7131d5c377bb6546593",
        }

        mock_collection.find.return_value = [{"metadata": metadata}]

        result = self.client.insert_document(document, "test_filename")
        mock_collection.insert_one.assert_not_called()
        self.assertEqual(result, None)
        self.mock_connect.assert_called_once()
        mock_collection.find.assert_called_once_with(
            {"metadata.filename": {"$eq": "test_filename"}}, {"metadata": 1}
        )

    @patch.object(MongoDBClient, "collection")
    def test_insert_document_with_existing_no_match(self, mock_collection):
        document = {"test_key": "test_value"}
        metadata = {
            "filename": "test_filename",
            "date": datetime.datetime.now(),
            "hash": "test_hash",
        }

        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "a49fe606-c777-41e2-a3e1-2f63a7691e18"
        mock_collection.insert_one.return_value = mock_insert_result
        mock_collection.find.return_value = [{"metadata": metadata}]

        result = self.client.insert_document(document, "test_filename")
        mock_collection.insert_one.assert_called_once_with(document)
        self.assertEqual(result, mock_insert_result.inserted_id)
        self.mock_connect.assert_called_once()
        mock_collection.find.assert_called_once_with(
            {"metadata.filename": {"$eq": "test_filename"}}, {"metadata": 1}
        )

    @patch.object(MongoDBClient, "collection")
    @patch("logging.getLogger")
    def test_insert_document_with_existing_ill_formed(self, mock_getLogger, mock_collection):
        document = {"test_key": "test_value"}
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_id = "a49fe606-c777-41e2-a3e1-2f63a7691e18"
        mock_collection.insert_one.return_value = mock_insert_result
        mock_collection.find.return_value = [{"metadata": "test_string"}]
        mock_logger = MagicMock()
        mock_getLogger.return_value = mock_logger

        result = self.client.insert_document(document, "test_filename")
        mock_collection.insert_one.assert_called_once_with(document)
        self.assertEqual(result, mock_insert_result.inserted_id)
        self.mock_connect.assert_called_once()
        mock_collection.find.assert_called_once_with(
            {"metadata.filename": {"$eq": "test_filename"}}, {"metadata": 1}
        )

        mock_logger.exception.assert_called_once()

    @patch.object(MongoDBClient, "collection")
    @patch("logging.getLogger")
    @patch("time.sleep")
    def test_insert_document_with_auto_reconnect_error(self, mock_sleep, mock_getLogger,mock_collection):
        document = {"test_key": "test_value"}
        mock_collection.find.return_value = []
        mock_collection.insert_one.side_effect = pymongo.errors.AutoReconnect()
        mock_logger = MagicMock()
        mock_getLogger.return_value = mock_logger
        result = self.client.insert_document(document, "test_filename")

        self.assertEqual(mock_collection.insert_one.call_count, 2)
        self.assertEqual(result, None)
        self.assertEqual(mock_collection.find.call_count, 2)
        self.assertEqual(mock_logger.exception.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)
        self.assertEqual(self.mock_connect.call_count, 2)

    @patch.object(MongoDBClient, "collection")
    @patch("logging.getLogger")
    @patch("time.sleep")
    def test_insert_document_with_operation_failure_error(self, mock_sleep, mock_getLogger, mock_collection):
        document = {"test_key": "test_value"}
        mock_collection.find.return_value = []
        mock_collection.insert_one.side_effect = pymongo.errors.OperationFailure("test failure")
        mock_logger = MagicMock()
        mock_getLogger.return_value = mock_logger
        result = self.client.insert_document(document, "test_filename")

        self.assertEqual(mock_collection.insert_one.call_count, 2)
        self.assertEqual(result, None)
        self.assertEqual(mock_collection.find.call_count, 2)
        self.assertEqual(mock_logger.exception.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)
        self.mock_connect.assert_called_once()

    @patch.object(MongoDBClient, "collection")
    def test_insert_documents(self, mock_collection):
        documents = [{"test_key": "test_value"}, {"another_key": "another_value"}]

        # Set up the mock to return a specific result
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_ids = [
            "d51bdcdf-bf1a-4cf7-a366-999fae27f3bf",
            "6635c36d-a760-43fc-8a85-31382e26cd18",
        ]
        mock_collection.insert_many.return_value = mock_insert_result
        mock_collection.find.return_value = []
        result = self.client.insert_documents(documents, "test_filename")
        mock_collection.insert_many.assert_called_once()
        self.assertEqual(result, mock_insert_result.inserted_ids)
        self.mock_connect.assert_called_once()
        mock_collection.find.assert_called_once_with(
            {"metadata.filename": "test_filename"}, {"metadata": 1}
        )

    @patch("gridfs.GridFS")
    @patch("pymongo.MongoClient")
    def test_insert_file(self, mock_mongo_client, mock_gridfs):
        mock_db = MagicMock()
        mock_mongo_client.return_value = {"test_db": mock_db}
        mock_gridfs.return_value.find.return_value = []

        # SHA-256 of b'test_data'
        expected_hash = "e7d87b738825c33824cf3fd32b7314161fc8c425129163ff5e7260fc7288da36"

        self.client.insert_file(b"test_data", "test_filename")

        # Check that find was called with correct arguments
        mock_gridfs.return_value.find.assert_called_once_with({"filename": "test_filename"})

        # Check that put was called with correct arguments.
        # Note that we're only checking the hash of the data, as the date will vary.
        _, kwargs = mock_gridfs.return_value.put.call_args
        self.assertEqual(kwargs["filename"], "test_filename")
        self.assertEqual(kwargs["metadata"]["hash"], expected_hash)
        self.assertIsInstance(kwargs["metadata"]["date"], datetime.datetime)
        self.mock_connect.assert_called_once()

    def test_with_db(self):
        new_client = self.client.with_db("new_db")
        self.assertEqual(new_client._db_name, self.client._db_name)

    def test_with_collection(self):
        new_client = self.client.with_collection("new_collection")
        self.assertEqual(new_client._collection_name, self.client._collection_name)


if __name__ == "__main__":
    unittest.main()
