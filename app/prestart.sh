#! /usr/bin/env bash

echo "Running inside /app/prestart.sh."

python /app/app/prepare/prepare_all.py

sleep 5;

echo "prestart.sh finished"
