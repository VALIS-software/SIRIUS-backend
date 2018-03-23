#!/bin/bash

set -e
SHA=$(echo $(date) | sha1sum | awk '{ print $1 }')
sudo docker build -t us.gcr.io/valis-194104/sirius:$SHA .
sudo docker tag us.gcr.io/valis-194104/sirius:$SHA us.gcr.io/valis-194104/sirius:prod
sudo gcloud docker -- push us.gcr.io/valis-194104/sirius:$SHA
kubectl patch deployment sirius-prod -p '{"spec":{"template":{"spec":{"containers":[{"name":"sirius-prod","image":"us.gcr.io/valis-194104/sirius:'"$SHA"'"}]}}}}'
