# SIRIUS

## File Docs

### circle.yml:
    Incorporated with CircleCI
    This configuration will allow CircleCI container to build, test and deploy the server app to out Google Cloud project.
    ssh key was provided to circleci.com, so git commit will trigger auto build.
    New Google Cloud serivce account was set, and json key was encrypted by base64 and provided to circleci.com.

### Dockerfile:
    Configuration for Docker.
    Python 3.6 was used.

### requirements.txt:
    Loaded by Docker to setup running environment.

### deploy.sh:
    Loaded by CircleCI.
    Push newly built Docker image to Google Cloud Container Registry.

### scr/app.py:
    Main program to launch server.

## Configurations
### Kubenetes Configurations (Ran once):
#### Initialize deployment
    `kubectl run sirius --image=us.gcr.io/valis-194104/sirius:latest --port 5000` 
#### Expose port
    `kubectl expose deployment sirius --type=LoadBalancer --port 80 --target-port 5000` 
