#!/bin/bash

# Exit on any error
set -e

sudo /opt/google-cloud-sdk/bin/gcloud docker push us.gcr.io/${PROJECT_NAME}/sirius
sudo chown -R ubuntu:ubuntu /home/ubuntu/.kube
kubectl patch deployment docker-sirius -p '{"spec":{"template":{"spec":{"containers":[{"name":"sirius","image":"us.gcr.io/VALIS/sirius:'"$CIRCLE_SHA1"'"}]}}}}'

