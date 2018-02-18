# SIRIUS

SIRIUS is the backend for VALIS, a next generation genome visualization and exploration tool. SIRIUS provides a graph API to search genomic metadata, signals, and annotations. SIRIUS is built on top of TileDB & MongoDB.

## Getting Started

To run SIRIUS you must install [Docker](https://www.docker.com/get-docker). Once you've setup docker go into the source code folder and run:
```
docker build .
```
NOTE: This step may take a while to complete as several packages need to be downloaded.

Once the build is completed you should see a success message like this:
```
Successfully built 0d02bb51d255
```
Copy the hash and run the following, to start SIRIUS:
```
docker run -p 5000:5000 0d02bb51d255
```
The `-p 5000:5000` maps the internal flask servers port (5000) to the 5000 port on your local machine.

Navigate to `http://localhost:5000` and you should see the VALIS frontend show up.

## Configuration Files

### circle.yml:
    Incorporated with CircleCI
    This configuration will allow CircleCI container to build, test and deploy the server app to our Google Cloud project.
    ssh key was provided to circleci.com, so git commit will trigger auto build.
    New Google Cloud service account was set, and json key was encrypted by base64 and provided to circleci.com.

### Dockerfile:
    Configuration for Docker.
    Python 3.6 was used.

### requirements.txt:
    Loaded by Docker to setup the runtime environment.

### deploy.sh:
    Loaded by CircleCI.
    Pushes newly built Docker image to Google Cloud Container Registry.

### scr/app.py:
    Main program to launch the SIRIUS server.

## Deployment
### Kubenetes Configurations (Only needs to run once):
#### Start a deployment
    `kubectl run sirius --image=us.gcr.io/valis-194104/sirius:latest --port 5000` 
#### Expose port
    `kubectl expose deployment sirius --type=LoadBalancer --port 80 --target-port 5000` 
