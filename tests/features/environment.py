"""
Behave test environment configuration.

Copyright 2023 Jerrad Michael Genson

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""

import shutil

from steps.utils import start_mongodb


def before_scenario(context, scenario):
    start_mongodb(context)


def after_scenario(context, scenario):
    context.container.stop()
    if hasattr(context, "tmpdir"):
        shutil.rmtree(context.tmpdir)

    if hasattr(context, "tmpfile"):
        context.tmpfile.unlink()

    if hasattr(context, "process"):
        context.process.terminate()
