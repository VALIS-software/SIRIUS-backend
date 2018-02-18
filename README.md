# SIRIUS

SIRIUS is the backend for VALIS, a next generation genome visualization and exploration tool. SIRIUS provides a graph API to search genomic metadata, signals, and annotations. SIRIUS is built on top of TileDB & MongoDB.

## Getting Started

Option 1 (Recommended): Run Flask server locally

Install all of the dependencies:
* Flask
* Flask-CORS
* pymongo (for connecting to mongo server)
* pyensembl (Will be removed when we remove the mockAnnotation Data GRCh38_genes)
* google-cloud-storage (for calling `app/sirius/prepare/update_valis_webfront.py` to pull VALIS from Google Cloud)

The VALIS dist folder should be put at `app/sirius/valis-dist` for flask to find it. Calling `update_valis_webfront.py` will also do this for you.

Go to the directory `app/sirius` and type `FLASK_APP=main.py flask run`

Option 2 (More complex): Run inside Docker container

Install [Docker](https://www.docker.com/get-docker). Once you've setup docker go into the source code folder and run:
```
sudo docker build -t sirius-dev:latest .
```
NOTE: This step may take a while to complete as several packages need to be downloaded.

Once the build is completed you should see a success message like this:
```
Successfully built 3d65f2920032
Successfully tagged sirius-dev:latest
```
To start SIRIUS inside container:
```
docker run -p 5000:5000 sirius-dev:latest
```
The `-p 5000:5000` maps the internal flask servers port (5000) to the 5000 port on your local machine.
The container has been configured to use Nginx and uwsgi to launch Flask.

To launch terminal inside container (for debugging):
```
docker run -p 5000:5000 -ti sirius-dev:latest /bin/bash
```



    
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

### circle.yml:
    Incorporated with CircleCI
    This configuration will allow CircleCI container to build, test and deploy the server app to our Google Cloud project.
    ssh key was provided to circleci.com, so git commit will trigger auto build.
    New Google Cloud service account was set, and json key was encrypted by base64 and provided to circleci.com.

### Dockerfile:
    Configuration for Docker.
    Python 3.6 was used.

### deploy-dev.sh:
    Loaded by CircleCI. Pushes newly built Docker image to Google Cloud Container Registry.
    You don't usually call this script by yourself.

### app/prestart.sh:
    Called as ENTRYPOINT of Docker image, to pull VALIS and copy cache from persistent disk.

### app/uwsgi.ini:
    Picked up by uwsgi in Docker container.
