#!/bin/bash

set -e

sudo docker build -t us.gcr.io/valis-194104/sirius:prod
sudo gcloud docker -- push us.gcr.io/valis-194104/sirius:prod
# use an annotation of date to triggle an roll-out
d=$(echo $(date) | tr -d ' ') # remove space
kubectl patch deployment sirius-prod -p '{"spec":{"template":{"metadata":{"annotations":{"date":"'$d'"}}}}}'
