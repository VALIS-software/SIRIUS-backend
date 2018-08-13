#!/bin/bash

set -e
SHA=$(echo $(date) | sha1sum | awk '{ print $1 }')
docker build -t us.gcr.io/valis-194104/sirius:$SHA -f config/kubernetes/production/Dockerfile .
docker tag us.gcr.io/valis-194104/sirius:$SHA us.gcr.io/valis-194104/sirius:prod
gcloud docker -- push us.gcr.io/valis-194104/sirius:$SHA
gcloud docker -- push us.gcr.io/valis-194104/sirius:prod
kubectl patch deployment sirius-prod -p '{"spec":{"template":{"spec":{"containers":[{"name":"sirius-prod","image":"us.gcr.io/valis-194104/sirius:'"$SHA"'"}]}}}}' --namespace production
