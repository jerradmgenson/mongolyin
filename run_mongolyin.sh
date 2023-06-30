#!/bin/bash
# This script starts the mongolyin process, passing through any command line
# arguments to it, and restarts mongolyin whenever it exits with a 0 return
# status. The motivation for this is that the Docker container and/or Python
# memory manager sometimes continue to reserve memory that the app no longer
# needs, which can add up to a signficant amount over time. mongolyin solves
# this by restarting when the memory usage exceeds a certain amount and it
# is idle.
#
# Copyright 2023 Jerrad Michael Genson
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

while true; do
    python -m mongolyin.mongolyin "${@}"
    exit_code=$?

    if [[ exit_code -ne 0 ]]; then
        exit $exit_code
    fi
done
