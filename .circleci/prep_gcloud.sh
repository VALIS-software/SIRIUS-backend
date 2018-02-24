#!/bin/bash

export PROJECT_NAME="valis"
export PROJECT_ID="valis-194104"
export CLUSTER_NAME="cluster-sirius"
export CLOUDSDK_COMPUTE_ZONE="us-west1-b"


echo $GCLOUD_SERVICE_KEY | base64 --decode -i > ${HOME}//gcloud-service-key.json
gcloud auth activate-service-account --key-file ${HOME}/gcloud-service-key.json
gcloud config set project $PROJECT_ID
gcloud --quiet config set container/cluster $CLUSTER_NAME
gcloud config set compute/zone ${CLOUDSDK_COMPUTE_ZONE}
gcloud --quiet container clusters get-credentials $CLUSTER_NAME

