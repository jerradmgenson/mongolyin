"""
Unit tests for mongolyin.mongolyin

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import os
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import clevercsv
import numpy as np
import pandas as pd

from mongolyin import etl, mongolyin

PANDAS_EXTENSIONS = [".csv", ".parquet", ".xls", ".xlsx", ".xlsm", ".xlsb", ".odf", ".ods", ".odt"]
SPREADSHEET_EXTENSIONS = [".xls", ".xlsx", ".xlsm", ".xlsb", ".odf", ".ods", ".odt"]
TEST_JSON_DATA = '{"key": "value"}'
TEST_BIN_DATA = b"\x00\x01\x02\x03"
TEST_CSV_DATA = "col1,col2\nval1,val2"
TEST_INGRESS_PATH = Path("/path/to/ingress")
TEST_FILEPATH = TEST_INGRESS_PATH / "db/collection/file.csv"


class TestDispatch(unittest.TestCase):
    def setUp(self):
        self.mock_mongo_client = MagicMock()

    @patch.object(etl.Pipeline, "run")
    def test_create_dispatch(self, mock_run):
        dispatch, process = mongolyin.create_dispatch(
            self.mock_mongo_client,
            TEST_INGRESS_PATH,
            1000,
        )

        dispatch(TEST_FILEPATH)
        mock_run.assert_not_called()
        process()
        mock_run.assert_called_once()

    @patch.object(etl.Pipeline, "run")
    def test_dont_process_duplicate_event(self, mock_run):
        dispatch, process = mongolyin.create_dispatch(
            self.mock_mongo_client,
            TEST_INGRESS_PATH,
            1000,
        )

        dispatch(TEST_FILEPATH)
        dispatch(TEST_FILEPATH)
        mock_run.assert_not_called()
        process()
        process()
        mock_run.assert_called_once()


class TestSubPath(unittest.TestCase):
    def test_ingress_path_is_subset_of_filepath(self):
        expected_output = ["db", "collection", "file.csv"]
        self.assertEqual(mongolyin.sub_path(TEST_INGRESS_PATH, TEST_FILEPATH), expected_output)

    def test_filepath_is_subset_of_ingress_path(self):
        expected_output = ["db", "collection", "file.csv"]
        with self.assertRaises(ValueError):
            mongolyin.sub_path(TEST_FILEPATH, TEST_INGRESS_PATH)

    def test_ingress_path_is_filepath(self):
        expected_output = ["db", "collection", "file.csv"]
        self.assertEqual(mongolyin.sub_path(TEST_INGRESS_PATH, TEST_INGRESS_PATH), [])

    def test_ingress_path_filepath_mismatch(self):
        expected_output = ["db", "collection", "file.csv"]
        with self.assertRaises(ValueError):
            mongolyin.sub_path(Path("/tmp/test"), TEST_INGRESS_PATH)


class TestGetDBName(unittest.TestCase):
    def test_database_dir_exists(self):
        self.assertEqual(mongolyin.get_db_name(TEST_INGRESS_PATH, TEST_FILEPATH), "db")

    def test_database_dir_doesnt_exist(self):
        retval = mongolyin.get_db_name(TEST_INGRESS_PATH, TEST_INGRESS_PATH / "test.csv")
        self.assertIsNone(retval)


class TestGetCollectionName(unittest.TestCase):
    def test_collection_dir_exists(self):
        retval = mongolyin.get_collection_name(TEST_INGRESS_PATH, TEST_FILEPATH)
        self.assertEqual(retval, "collection")

    def test_collection_dir_doesnt_exist(self):
        retval = mongolyin.get_collection_name(TEST_INGRESS_PATH, TEST_INGRESS_PATH / "db/test.csv")
        self.assertEqual(retval, "misc")


class TestETLFunctions(unittest.TestCase):
    def setUp(self):
        self.mongo_client = MagicMock()

    def test_select_etl_functions_pandas(self):
        extract, load = mongolyin.select_etl_functions(TEST_FILEPATH, self.mongo_client, 1000)
        self.assertTrue(callable(mongolyin.extract_csv_chunks))
        self.assertTrue(callable(load))

    @patch("mongolyin.mongolyin.get_json_type")
    def test_select_etl_functions_json_dict(self, mock_get_json_type):
        mock_get_json_type.return_value = "dict"
        filepath = TEST_FILEPATH.with_suffix(".json")
        extract, load = mongolyin.select_etl_functions(filepath, self.mongo_client, 1000)
        self.assertEqual(extract, mongolyin.extract_json)
        self.assertTrue(callable(load))

    @patch("mongolyin.mongolyin.get_json_type")
    def test_select_etl_functions_json_list(self, mock_get_json_type):
        mock_get_json_type.return_value = "list"
        filepath = TEST_FILEPATH.with_suffix(".json")
        extract, load = mongolyin.select_etl_functions(filepath, self.mongo_client, 1000)
        self.assertTrue(callable(extract))
        self.assertTrue(callable(load))

    def test_select_etl_functions_bin(self):
        filepath = TEST_FILEPATH.with_suffix(".bin")
        extract, load = mongolyin.select_etl_functions(filepath, self.mongo_client, 1000)
        self.assertEqual(extract, mongolyin.extract_bin)
        self.assertTrue(callable(load))

    def test_update_mongodb_client_file_in_database_dir(self):
        updated_client = mongolyin.update_mongodb_client(
            self.mongo_client, TEST_INGRESS_PATH, TEST_FILEPATH
        )
        self.assertIsNotNone(updated_client)

    def test_update_mongodb_client_file_not_in_database_dir(self):
        updated_client = mongolyin.update_mongodb_client(
            self.mongo_client, TEST_INGRESS_PATH, TEST_INGRESS_PATH / "test.csv"
        )
        self.assertIsNone(updated_client)

    def test_extract_json(self):
        mock_file_descriptor = MagicMock()
        mock_file_descriptor.read.return_value = TEST_JSON_DATA
        mock_filepath = MagicMock()
        mock_filepath.open.return_value.__enter__.return_value = mock_file_descriptor
        data = mongolyin.extract_json(mock_filepath)
        self.assertEqual(data, {"key": "value"})

    def test_extract_bin(self):
        mock_file_descriptor = MagicMock()
        mock_file_descriptor.read.return_value = TEST_BIN_DATA
        mock_filepath = MagicMock()
        mock_filepath.open.return_value.__enter__.return_value = mock_file_descriptor
        data = mongolyin.extract_bin(mock_filepath)
        self.assertEqual(data, TEST_BIN_DATA)


class TestFileReadyCheck(unittest.TestCase):
    @patch("os.path.getsize")
    @patch("time.time")
    def test_file_ready_check_success(self, mock_time, mock_getsize):
        # Mock os.path.getsize to return 100 first time and 100 the second time
        mock_getsize.side_effect = [100, 100]

        # Mock time.time to simulate that less than the timeout has passed
        mock_time.side_effect = [0, 0.1, 0.2]

        # Call the function and check that it returns the expected value
        result = mongolyin.file_ready_check("dummy_file_path")
        self.assertEqual(result, "dummy_file_path")

    @patch("os.path.getsize")
    @patch("time.time")
    def test_file_ready_check_timeout(self, mock_time, mock_getsize):
        # Mock os.path.getsize to return different sizes each time
        mock_getsize.side_effect = [100, 200, 300]

        # Mock time.time to simulate that more than the timeout has passed
        mock_time.side_effect = [0, 1, 6]

        # Call the function and check that it raises a TimeoutError
        with self.assertRaises(TimeoutError):
            mongolyin.file_ready_check("dummy_file_path")

    @patch("os.path.getsize")
    @patch("time.time")
    def test_file_ready_check_os_error(self, mock_time, mock_getsize):
        # Mock os.path.getsize to raise an OSError
        mock_getsize.side_effect = OSError()

        # Mock time.time to simulate that less than the timeout has passed
        mock_time.side_effect = [t / 10 for t in range(60)]

        # Call the function and check that it doesn't raise an exception
        try:
            with self.assertRaises(TimeoutError):
                mongolyin.file_ready_check("dummy_file_path")

        except OSError:
            self.fail("file_ready_check raised OSError unexpectedly!")


class TestSetQueue(unittest.TestCase):
    def setUp(self):
        self.set_queue = mongolyin.SetQueue()

    def test_push(self):
        self.set_queue.push("item1")
        self.set_queue.push("item2")
        self.set_queue.push("item1")  # This should be ignored
        self.assertEqual(len(self.set_queue), 2)
        self.assertIn("item1", self.set_queue)
        self.assertIn("item2", self.set_queue)

    def test_pop(self):
        self.set_queue.push("item1")
        self.set_queue.push("item2")
        popped_item = self.set_queue.pop()
        self.assertEqual(popped_item, "item1")
        self.assertNotIn(popped_item, self.set_queue)

    def test_pop_empty(self):
        with self.assertRaises(IndexError):
            self.set_queue.pop()

    def test_contains(self):
        self.set_queue.push("item1")
        self.assertIn("item1", self.set_queue)
        self.assertNotIn("item2", self.set_queue)


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

        self.converted_docs = mongolyin.convert_strings_to_numbers(self.docs)

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

    def test_sparse_dataset(self):
        data = [
            {"a": "1", "b": "true", "c": "3.1"},
            {"a": "2", "b": "false", "d": "0"},
            {"a": "3", "c": "1.4", "d": "1"},
        ]

        converted_data = mongolyin.convert_strings_to_numbers(data)
        self.assertEqual(converted_data[0], {"a": 1, "b": True, "c": 3.1})
        self.assertEqual(converted_data[1], {"a": 2, "b": False, "d": False})
        self.assertEqual(converted_data[2], {"a": 3, "c": 1.4, "d": True})


class TestConvertBool(unittest.TestCase):
    def test_default(self):
        with self.assertRaises(TypeError):
            mongolyin.convert_bool(None)

    def test_convert_bool_from_string(self):
        self.assertTrue(mongolyin.convert_bool("true"))
        self.assertTrue(mongolyin.convert_bool("True"))
        self.assertTrue(mongolyin.convert_bool("TRUE"))

        self.assertFalse(mongolyin.convert_bool("false"))
        self.assertFalse(mongolyin.convert_bool("False"))
        self.assertFalse(mongolyin.convert_bool("FALSE"))

        with self.assertRaises(ValueError):
            mongolyin.convert_bool("random_string")

    def test_convert_bool_from_int(self):
        self.assertTrue(mongolyin.convert_bool(1))
        self.assertFalse(mongolyin.convert_bool(0))

        with self.assertRaises(ValueError):
            mongolyin.convert_bool(2)

        with self.assertRaises(ValueError):
            mongolyin.convert_bool(-1)

    def test_convert_bool_from_string_representation_of_int(self):
        self.assertTrue(mongolyin.convert_bool("1"))
        self.assertFalse(mongolyin.convert_bool("0"))

        with self.assertRaises(ValueError):
            mongolyin.convert_bool("2")

        with self.assertRaises(ValueError):
            mongolyin.convert_bool("-1")

    def test_convert_bool_from_bool(self):
        self.assertTrue(mongolyin.convert_bool(True))
        self.assertFalse(mongolyin.convert_bool(False))


class TestDecorators(unittest.TestCase):
    def setUp(self):
        self.data_gen = lambda: ({"value": str(i)} for i in range(10))
        self.autotyped_gen = mongolyin.autotyping(lambda: ([data] for data in self.data_gen()))
        self.chunked_gen = mongolyin.chunking(self.data_gen)
        self.autotyped_chunked_gen = mongolyin.autotyping(mongolyin.chunking(self.data_gen))

    def test_autotyping(self):
        data = list(self.autotyped_gen())
        self.assertEqual(
            data, [[{"value": False}], [{"value": True}]] + [[{"value": i}] for i in range(2, 10)]
        )

    def test_chunking(self):
        chunks = list(self.chunked_gen(chunk_size=2))
        self.assertEqual(
            chunks, [[{"value": str(i)}, {"value": str(i + 1)}] for i in range(0, 10, 2)]
        )

        all_in_one_chunk = list(self.chunked_gen(chunk_size=-1))
        self.assertEqual(all_in_one_chunk, [[{"value": str(i)} for i in range(10)]])

    def test_autotyping_and_chunking(self):
        chunks = list(self.autotyped_chunked_gen(chunk_size=2))
        self.assertEqual(chunks, [[{"value": i}, {"value": i + 1}] for i in range(0, 10, 2)])

        all_in_one_chunk = list(self.autotyped_chunked_gen(chunk_size=-1))
        self.assertEqual(all_in_one_chunk, [[{"value": i} for i in range(10)]])


if __name__ == "__main__":
    unittest.main()
