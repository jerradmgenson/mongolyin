"""
Utility functions for behave test steps.

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import os
import time


def wait_for_ready(logstream, timeout=30):
    """
    Block until mongolyin process to be ready to monitor directory changes.

    Args:
      logstream: A stream of log messages from mongolyin opened for reading.
      timeout (optional): Number of seconds to wait before timing out.

    Returns:
      True (always)

    Raises:
      TimeoutError: if the monoglyin process takes longer to be ready than the
                    time specified by `timeout`.

    """

    tick = time.time()
    while time.time() - tick < timeout:
        logstream.seek(0)
        if "mongolyin ready" in logstream.read():
            return True

        time.sleep(0.1)

    logstream.seek(0)
    logs = logstream.read()
    raise TimeoutError("Timed out waiting for mongolyin to be ready.\n\n" + logs)


def wait_for_upload(logstream, filenames, timeout=60):
    """
    Block until mongolyin process has uploaded all expected files.

    Args:
      logstream: A stream of log messages from mongolyin opened for reading.
      filenames: A sequence of filenames that are expected to be uploaded.
      timeout (optional): Number of seconds to wait before timing out.

    Returns:
      True (always)

    Raises:
      TimeoutError: if the monoglyin process takes longer to be ready than the
                    time specified by `timeout`.

    """

    tick = time.time()
    while time.time() - tick < timeout:
        logstream.seek(0)
        logs = logstream.read()
        if all([f"'{f}' inserted into database" in logs for f in filenames]):
            return True

        time.sleep(0.1)

    raise TimeoutError("Timed out waiting for mongolyin to upload files.\n\n" + logs)


def read_filenames(directory):
    """
    Read filenames recursively from `directory`.

    Returns:
      A generator of filenames from `directory`.

    """

    for _, _, files in os.walk(directory):
        for file_ in files:
            yield file_
