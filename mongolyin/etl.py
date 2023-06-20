"""
Defines a simple ETL framework that is used to extract, transform, and
load data using a pipeline.

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""


import logging
from typing import Callable


class Stage:
    """
    Defines a stage in the ETL pipeline.

    Attributes:
        name (str): The name of the stage.
        operation (Callable): The function to be executed in this stage. This function should take
            the data as input and return the transformed data.

    """

    def __init__(self, name: str, operation: Callable):
        self.name = name
        self.operation = operation


class Pipeline:
    """
    ETL Pipeline.

    Attributes:
        *stages (Stage): The stages in the pipeline.
    """

    def __init__(self, *stages: Stage):
        self.stages = stages

    def run(self, data):
        """
        Runs the ETL pipeline.

        Args:
            data: The data to be processed.

        Returns:
            The transformed data after all stages have been executed.

        Raises:
            ETLException: If an exception occurs in any stage, the exception is caught, logged,
                and re-raised as an ETLException.

        """

        logger = logging.getLogger(__name__)
        for stage in self.stages:
            try:
                logger.info("Running stage '%s'...", stage.name)
                data = stage.operation(data)

            except Exception as e:
                logger.exception(e)
                raise ETLException(stage.name, e) from e

        return data


class ETLException(Exception):
    """
    Custom exception to include information about the stage and the original exception.

    Attributes:
        stage_name (str): The name of the stage in which the exception occurred.
        original_exception (Exception): The original exception that was raised.
    """

    def __init__(self, stage_name: str, original_exception: Exception):
        super().__init__(f"Exception occurred at stage '{stage_name}': {str(original_exception)}")
        self.stage_name = stage_name
        self.original_exception = original_exception
