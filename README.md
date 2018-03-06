# SIRIUS

SIRIUS is the backend for VALIS, a next generation genome visualization and exploration tool. SIRIUS provides a graph API to search genomic metadata, signals, and annotations. SIRIUS is built on top of TileDB & MongoDB.

## Getting Started

Option 1 (Recommended): Run Flask server locally

Install all of the dependencies:
* Flask
* Flask-CORS
* Scipy
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

## Setup local MongoDB server

To setup a local MongoDB server, as a copy of the cloud MongoDB server, but responding much faster.

1. Launch local mongo docker image.

```
sudo docker run -p 27017:27017 --name some-mongo -d mongo --auth
```

This will download the official mongo container image and run it in detached mode with the name `some-mongo`.

2. Setup initial admin user in local MongoDB.

First, launch a bash shell inside mongo container
```
sudo docker exec -it some-mongo bash
```
you should get a bash terminal inside the container, like `root@093275de44e7:/# `

then run initial mongo shell

```
mongo admin
```

you should get a mongo shell like `>`,

then create initial admin user

```
db.createUser({user: 'admin', pwd: 'password', roles:[{role: 'root', db: 'admin'}] })
```

The username and password can be changed here to your own choice. 

After this you should see a message like 

```
Successfully added user: {
	"user" : "admin",
	"roles" : [
		{
			"role" : "root",
			"db" : "admin"
		}
	]
}
```

3. Setup user for sirius

First exit the current mongo shell

```
> exit
```

Then login again with the just-created admin user

```
mongo -u admin -p password --authenticationDatabase admin
```

If successful, you should get a new mongo shell like `>`

Then switch to a new database called `testdb`, and create the user `sirius` there:

```
use testdb
db.createUser({user: 'sirius', pwd: 'valis', roles:[{role: 'readWrite', db: 'testdb'}, {role: 'readWrite', db:'database'}, {role: 'readWrite', db:'database1'}] })
```

After this step, you should see another `Successfully added user` message, and your MongoDB container is ready to go!

Please be noted that this MongoDB server doesn't have any data yet. You can use the scripts in `app/sirius/tools/` to do that.

## Connect sirius to local MongoDB server

With the local MongoDB server up and running, the local sirius server ran by `flask run` in the `app/sirius` folder should work automatically by connecting to the MongoDB server.

If you are running SIRIUS inside a docker container, one extra argument is needed to provide the link to the local MongoDB server, so you will launch SIRIUS docker container by

```
docker run -p 5000:5000 --link some-mongo:mongo sirius-dev:latest
```

Here `some-mongo` is the name of the mongo container we just created.

Have fun!

    
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
