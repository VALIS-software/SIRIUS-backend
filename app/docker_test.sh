#!/usr/bin/env bash

echo "Running SIRIUS tests inside Docker container"

/app/prep_gcloud.sh

kubectl port-forward mongo-0 27017:27017 &

cd /app/sirius/tests

python -m unittest discover -q

pkill kubectl

