# SIRIUS

SIRIUS is the backend for VALIS, a next generation genome visualization and exploration tool. SIRIUS provides a graph API to search genomic metadata, signals, and annotations. SIRIUS is built on top of TileDB & MongoDB.

## Getting Started

Please see Dev-Setup.md for a complete instruction for setting up the SIRIUS server as well as MongoDB server as docker containers.

## Configuration Files
Configurations are located in the config/ folder

### config/build_docker:
    Files used to build the reference Docker image yudongdev/uwsgi-nginx-flask:sirius
    This Docker image was hand-craft by Yudong and you normally don't need to do this yourself.

### config/kubernetes:
    Configuration used to create Kubernetes deployment, and expose sirius-dev service

### config/mongodb:
    Configurations used to create MongoDB container on Kubernetes cluster.

## Other Files

### .circleci/:
    Incorporated with CircleCI
    This configuration will allow CircleCI container to build, test and deploy the server app to our Google Cloud project.
    ssh key was provided to circleci.com, so git commit will trigger auto build.
    New Google Cloud service account was set, and json key was encrypted by base64 and provided to circleci.com.

### Dockerfile:
    Configuration for Docker.
    Python 3.6 was used.

### app/prestart.sh:
    Called as ENTRYPOINT of Docker image, to pull VALIS and copy cache from persistent disk.

### app/uwsgi.ini:
    Picked up by uwsgi in Docker container.
