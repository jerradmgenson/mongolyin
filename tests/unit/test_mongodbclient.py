"""
Unit tests for mongolyin.mongodbclient

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import datetime
import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pymongo

from mongolyin.mongodbclient import (
    MongoDBClient,
    convert_bool,
    convert_strings_to_numbers,
)


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

        self.mock_client = MagicMock()
        self.client._client = self.mock_client

    def tearDown(self):
        self.patcher.stop()

    def test_context_manager(self):
        mock_client = MagicMock()
        client_args = (
            "test_address",
            "test_user",
            "test_pass",
            "test_auth_db",
            "test_db",
            "test_collection",
        )

        with MongoDBClient(*client_args, client=mock_client) as mongo_client:
            self.assertIsInstance(mongo_client, MongoDBClient)

        mock_client.close.assert_called_once()

    @patch("pymongo.MongoClient")
    def test_connect_closes_existing_client(self, mock_mongo_client):
        self.patcher.stop()
        client_args = (
            "test_address",
            "test_user",
            "test_pass",
            "test_auth_db",
            "test_db",
            "test_collection",
        )

        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        mongo_client = MongoDBClient(*client_args)
        mongo_client._connect()
        mongo_client._connect()
        self.assertEqual(mock_mongo_client.call_count, 2)
        mock_client.close.assert_called_once()

    @patch("logging.getLogger")
    def test_log_exception_on_close_error(self, mock_getLogger):
        mock_logger = MagicMock()
        mock_getLogger.return_value = mock_logger
        mock_close = MagicMock(side_effect=pymongo.errors.AutoReconnect())
        self.mock_client.close = mock_close
        self.client.close()
        mock_close.assert_called_once()
        mock_logger.exception.assert_called_once()

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
        mock_collection.find.assert_called_once_with(
            {"metadata.filename": {"$eq": "test_filename"}}, {"metadata": 1}
        )

        mock_logger.error.assert_called_once()

    @patch.object(MongoDBClient, "collection")
    def test_insert_documents_no_existing(self, mock_collection):
        documents = [{"test_key": "test_value"}, {"another_key": "another_value"}]

        # Set up the mock to return a specific result
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_ids = [
            "d51bdcdf-bf1a-4cf7-a366-999fae27f3bf",
            "6635c36d-a760-43fc-8a85-31382e26cd18",
        ]
        mock_collection.insert_many.return_value = mock_insert_result
        mock_collection.find.return_value = []
        result = self.client.insert_document(documents, "test_filename")
        mock_collection.insert_many.assert_called_once()
        self.assertEqual(result, mock_insert_result.inserted_ids)

    @patch.object(MongoDBClient, "collection")
    def test_insert_documents_existing_match(self, mock_collection):
        documents = [{"test_key": "test_value"}, {"another_key": "another_value"}]

        # Set up the mock to return a specific result
        mock_collection.find.return_value = documents
        result = self.client.insert_document(documents, "test_filename")
        mock_collection.insert_many.assert_not_called()
        self.assertEqual(result, [])

    @patch.object(MongoDBClient, "collection")
    def test_insert_documents_existing_no_match(self, mock_collection):
        documents = [{"test_key": "test_value"}, {"another_key": "another_value"}]

        # Set up the mock to return a specific result
        mock_insert_result = MagicMock()
        mock_insert_result.inserted_ids = [
            "d51bdcdf-bf1a-4cf7-a366-999fae27f3bf",
            "6635c36d-a760-43fc-8a85-31382e26cd18",
        ]
        mock_collection.insert_many.return_value = mock_insert_result
        expected_hashes = [
            "291ac4664cc84d2546fff3b17aaaf709c4f147d10da697e3263a56db9030f423",
            "0066b0e38e1ace9a791b4f1d33289950032f696213af35717e2f7c96a219b7de",
        ]

        return_value = []
        for exp_hash in expected_hashes:
            return_value.append({"metadata": {"hash": exp_hash}})

        mock_collection.find.return_value = return_value
        result = self.client.insert_document(documents, "test_filename")
        mock_collection.insert_many.assert_called_once()
        self.assertEqual(result, mock_insert_result.inserted_ids)

    @patch("gridfs.GridFS")
    @patch("pymongo.MongoClient")
    @patch.object(MongoDBClient, "client")
    def test_insert_file_no_existing(self, mock_client, mock_mongo_client, mock_gridfs):
        mock_db = MagicMock()
        mock_mongo_client.return_value = {"test_db": mock_db}
        mock_gridfs.return_value.find.return_value = []

        # SHA-256 of b'test_data'
        expected_hash = "e7d87b738825c33824cf3fd32b7314161fc8c425129163ff5e7260fc7288da36"

        self.client.insert_file(b"test_data", "test_filename")

        # Check that find was called with correct arguments
        mock_gridfs.return_value.find.assert_called_once_with(
            {"filename": "test_filename", "metadata.hash": expected_hash}
        )

        # Check that put was called with correct arguments.
        # Note that we're only checking the hash of the data, as the date will vary.
        _, kwargs = mock_gridfs.return_value.put.call_args
        self.assertEqual(kwargs["filename"], "test_filename")
        self.assertEqual(kwargs["metadata"]["hash"], expected_hash)
        self.assertIsInstance(kwargs["metadata"]["date"], datetime.datetime)

    @patch("gridfs.GridFS")
    @patch("pymongo.MongoClient")
    @patch.object(MongoDBClient, "client")
    def test_insert_file_existing_match(self, mock_client, mock_mongo_client, mock_gridfs):
        # SHA-256 of b'test_data'
        expected_hash = "e7d87b738825c33824cf3fd32b7314161fc8c425129163ff5e7260fc7288da36"

        mock_db = MagicMock()
        mock_mongo_client.return_value = {"test_db": mock_db}
        mock_gridfs.return_value.find.return_value = [{"metadata": {"hash": expected_hash}}]

        return_value = self.client.insert_file(b"test_data", "test_filename")
        self.assertEqual(return_value, None)

        # Check that find was called with correct arguments
        mock_gridfs.return_value.find.assert_called_once_with(
            {"filename": "test_filename", "metadata.hash": expected_hash}
        )
        mock_gridfs.return_value.put.assert_not_called()

    @patch("gridfs.GridFS")
    @patch("pymongo.MongoClient")
    @patch.object(MongoDBClient, "client")
    def test_insert_file_existing_no_match(self, mock_client, mock_mongo_client, mock_gridfs):
        mock_db = MagicMock()
        mock_mongo_client.return_value = {"test_db": mock_db}
        mock_gridfs.return_value.find.return_value = []

        return_value = self.client.insert_file(b"test_data", "test_filename")
        self.assertNotEqual(return_value, None)

        # Check that find was called with correct arguments
        mock_gridfs.return_value.put.assert_called_once()

    def test_with_db_existing(self):
        new_client = self.client.with_db("new_db")
        self.assertEqual(new_client._db_name, self.client._db_name)

    def test_with_db_no_existing(self):
        self.client._db_name = ""
        new_client = self.client.with_db("new_db")
        self.assertEqual(new_client._db_name, "new_db")

    def test_with_collection_existing(self):
        new_client = self.client.with_collection("new_collection")
        self.assertEqual(new_client._collection_name, self.client._collection_name)

    def test_with_collection_no_existing(self):
        self.client._collection_name = ""
        new_client = self.client.with_collection("new_collection")
        self.assertEqual(new_client._collection_name, "new_collection")


class TestConvertBool(unittest.TestCase):
    def test_default(self):
        with self.assertRaises(TypeError):
            convert_bool(None)

    def test_convert_bool_from_string(self):
        self.assertTrue(convert_bool("true"))
        self.assertTrue(convert_bool("True"))
        self.assertTrue(convert_bool("TRUE"))

        self.assertFalse(convert_bool("false"))
        self.assertFalse(convert_bool("False"))
        self.assertFalse(convert_bool("FALSE"))

        with self.assertRaises(ValueError):
            convert_bool("random_string")

    def test_convert_bool_from_int(self):
        self.assertTrue(convert_bool(1))
        self.assertFalse(convert_bool(0))

        with self.assertRaises(ValueError):
            convert_bool(2)

        with self.assertRaises(ValueError):
            convert_bool(-1)

    def test_convert_bool_from_string_representation_of_int(self):
        self.assertTrue(convert_bool("1"))
        self.assertFalse(convert_bool("0"))

        with self.assertRaises(ValueError):
            convert_bool("2")

        with self.assertRaises(ValueError):
            convert_bool("-1")

    def test_convert_bool_from_bool(self):
        self.assertTrue(convert_bool(True))
        self.assertFalse(convert_bool(False))


class TestConvertStringsToNumbers(unittest.TestCase):
    """
    Unit tests for mongodbclient.convert_strings_to_numbers

    """

    def setUp(self):
        self.docs = [
            {
                "a": "1",
                "b": "1,1",
                "c": "true",
                "d": "1",
                "e": "",
                "f": "0",
                "g": "0",
                "h": "1",
                "i": 1,
                "j": None,
                "k": 1.0,
                "l": True,
                "m": "true",
            },
            {
                "a": "2",
                "b": "2,2",
                "c": "false",
                "d": "2",
                "e": "NAN",
                "f": "1",
                "g": "1",
                "h": "2",
                "i": 2,
                "j": "3.1",
                "k": 2.0,
                "l": True,
                "m": "false",
            },
            {
                "a": "3",
                "b": "3,3",
                "c": "true",
                "d": "3.3",
                "e": "nan",
                "f": "0",
                "g": "2",
                "h": "q",
                "i": 3,
                "j": "",
                "k": 3.1,
                "l": False,
                "m": 5,
            },
        ]
        self.converted_docs = convert_strings_to_numbers(self.docs)

    def test_convert_strings_to_int(self):
        for doc in self.converted_docs:
            self.assertTrue(isinstance(doc["a"], int))

    def test_ints_are_left_alone(self):
        for doc in self.converted_docs:
            self.assertTrue(isinstance(doc["i"], int))

    def test_floats_are_left_alone(self):
        for doc in self.converted_docs:
            self.assertTrue(isinstance(doc["k"], float))

    def test_bools_are_left_alone(self):
        for doc in self.converted_docs:
            self.assertTrue(isinstance(doc["l"], bool))

    def test_convert_strings_with_commas_to_float(self):
        for doc in self.converted_docs:
            self.assertTrue(isinstance(doc["b"], float))

    def test_convert_int_and_float_strings_to_float(self):
        for doc in self.converted_docs:
            self.assertTrue(isinstance(doc["d"], float))

    def test_convert_true_false_strings_to_bool(self):
        for doc in self.converted_docs:
            self.assertTrue(isinstance(doc["c"], bool))

    def test_convert_0_1_strings_to_bool(self):
        for doc in self.converted_docs:
            self.assertTrue(isinstance(doc["f"], bool))

    def test_missing_values_to_none(self):
        for doc in self.converted_docs:
            self.assertIsNone(doc["e"])

    def test_almost_bool_string_to_int(self):
        for doc in self.converted_docs:
            self.assertTrue(isinstance(doc["g"], int))

    def test_unconvertible_fields1(self):
        for doc in self.converted_docs:
            self.assertTrue(isinstance(doc["h"], str))

    def test_unconvertible_fields2(self):
        self.assertTrue(isinstance(self.converted_docs[0]["m"], str))
        self.assertTrue(isinstance(self.converted_docs[1]["m"], str))
        self.assertTrue(isinstance(self.converted_docs[2]["m"], int))

    def test_convert_string_with_missing_values_to_float(self):
        self.assertIsNone(self.converted_docs[0]["j"])
        self.assertTrue(isinstance(self.converted_docs[1]["j"], float))
        self.assertIsNone(self.converted_docs[2]["j"])


if __name__ == "__main__":
    unittest.main()
