#!/usr/bin/env bash

echo "Running SIRIUS tests inside Docker container"

cd /app/sirius/tests

python -m unittest discover -q

