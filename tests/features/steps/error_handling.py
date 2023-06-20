"""
Behave steps for error_handling.feature

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import tempfile
import time
import re
import shutil
import subprocess
from pathlib import Path

import utils


@given(u'that we have a directory structure with a corrupt json file')
def step_impl(context):
   context.tmpdir = Path(tempfile.mkdtemp())
   collection_dir = context.tmpdir / "db" / "collection"
   collection_dir.mkdir(parents=True)
   corrupt_file = collection_dir / "corrupt.json"
   context.corrupt_file = corrupt_file
   data = "{'val': 123}"
   with corrupt_file.open("w") as fp:
       fp.write(data)


@when(u'we run mongolyin.py on that directory')
def step_impl(context):
    text = context.text.format(
        inputdir=context.tmpdir,
        address=context.mongo_address,
        username=context.mongo_username,
        password=context.mongo_password,
    )

    args = list(re.split(r"\s+", text))
    args = ["python", "-m", "mongolyin.mongolyin"] + args
    with tempfile.TemporaryFile("w+") as stderr:
        try:
            process = subprocess.Popen(args, stderr=stderr)
            utils.wait_for_ready(stderr)
            shutil.copyfile(
                context.corrupt_file,
                context.tmpdir / "db" / "collection" / "data1.json",
            )

            time.sleep(2)

        finally:
            process.terminate()

        stderr.seek(0)
        context.mongolyin_logs = stderr.read()


@then(u'it should log the error and continue without crashing')
def step_impl(context):
    raise NotImplementedError()
