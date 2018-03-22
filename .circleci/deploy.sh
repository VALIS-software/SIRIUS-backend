#!/bin/bash

# Exit on any error
set -e

# only push when the git branch is master
if [ "${CIRCLE_BRANCH}" == "master" ]; then
  gcloud docker -- push us.gcr.io/valis-194104/sirius
  kubectl patch deployment sirius-dev -p '{"spec":{"template":{"spec":{"containers":[{"name":"sirius-dev","image":"us.gcr.io/valis-194104/sirius:'"$CIRCLE_SHA1"'"}]}}}}'
fi
