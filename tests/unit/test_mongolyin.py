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
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from mongolyin import mongolyin

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
        self.mock_create_graph = MagicMock()
        self.mock_run_graph = MagicMock()

    def test_create_dispatch(self):
        dispatch, process = mongolyin.create_dispatch(
            self.mock_mongo_client,
            TEST_INGRESS_PATH,
            create_graph=self.mock_create_graph,
            run_graph=self.mock_run_graph,
        )

        dispatch(TEST_FILEPATH)
        self.mock_create_graph.assert_not_called()
        self.mock_run_graph.assert_not_called()
        process()
        self.mock_create_graph.assert_called_once()
        self.mock_run_graph.assert_called_once()

    def test_dont_process_duplicate_event(self):
        dispatch, process = mongolyin.create_dispatch(
            self.mock_mongo_client,
            TEST_INGRESS_PATH,
            create_graph=self.mock_create_graph,
            run_graph=self.mock_run_graph,
        )

        dispatch(TEST_FILEPATH)
        dispatch(TEST_FILEPATH)
        self.mock_create_graph.assert_not_called()
        self.mock_run_graph.assert_not_called()
        process()
        process()
        self.mock_create_graph.assert_called_once()
        self.mock_run_graph.assert_called_once()


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
        extract, load = mongolyin.select_etl_functions(TEST_FILEPATH, self.mongo_client)
        self.assertEqual(extract, mongolyin.extract_pandas)
        self.assertTrue(callable(load))

    def test_select_etl_functions_json(self):
        filepath = TEST_FILEPATH.with_suffix(".json")
        extract, load = mongolyin.select_etl_functions(filepath, self.mongo_client)
        self.assertEqual(extract, mongolyin.extract_json)
        self.assertTrue(callable(load))

    def test_select_etl_functions_bin(self):
        filepath = TEST_FILEPATH.with_suffix(".bin")
        extract, load = mongolyin.select_etl_functions(filepath, self.mongo_client)
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


if __name__ == "__main__":
    unittest.main()
