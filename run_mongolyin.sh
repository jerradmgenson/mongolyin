#!/bin/bash

# "${@}" are all the arguments

while true; do
    python -m mongolyin.mongolyin "${@}"
    exit_code=$?

    if [[ exit_code -ne 0 ]]; then
        exit $exit_code
    fi
done
