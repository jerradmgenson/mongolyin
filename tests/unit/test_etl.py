"""
Unit tests for mongolyin.etl

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""


import unittest

from mongolyin.etl import ETLException, Pipeline, Stage


class TestETL(unittest.TestCase):
    def setUp(self):
        self.stage1 = Stage("stage1", lambda x: x * 2)
        self.stage2 = Stage("stage2", lambda x: x - 1)
        self.stage3 = Stage("stage3", lambda x: x / 0)  # This stage will cause an error
        self.pipeline = Pipeline(self.stage1, self.stage2, self.stage3)

    def test_pipeline(self):
        # Test that the pipeline works correctly when no errors are present
        pipeline_no_errors = Pipeline(self.stage1, self.stage2)
        result = pipeline_no_errors.run(2)
        self.assertEqual(result, 3)

    def test_etl_exception(self):
        # Test that an ETLException is raised when an error occurs in a stage
        with self.assertRaises(ETLException) as cm:
            self.pipeline.run(2)

        exception = cm.exception
        self.assertEqual(exception.stage_name, "stage3")


if __name__ == "__main__":
    unittest.main()
