## SIRIUS Developer Playbook
---
### Before Start

1. Install google cloud SDK:

    https://cloud.google.com/sdk/

2. Initialize gcloud SDK and authenticate:

    `gcloud init`

3. Install Kubernetes:

    https://kubernetes.io/docs/tasks/tools/install-kubectl/

4. Authenticate kubectl:

    `gcloud container clusters get-credentials cluster-sirius`

---

### Monitor the current status

- `kubectl get pod`

    You should see two or more pods with status "Running". One called "mongo-0" is running the MongoDB container. The other called "sirius-dev-**********-*****", is running the SIRIUS server. These names are going to be used later.

    Note: The `pod` in Kubernetes has the same concept as a Docker container.

- `kubectl get service`

    Running services and their EXTERNAL-IP are shown here.

---

### Launch a terminal in the containers

- `kubectl exec -ti POD_NAME bash`

    `POD_NAME` is the name of the pod to log in. i.e.

    `kubectl exec -ti mongo-0 bash`

    Will open a bash terminal inside the `mongo-0` pod. You can then run the Mongo shell their. (MongoDB authentication needed)

    `kubectl exec -ti sirius-pod-***** bash`

    Replace the `sirius-pod-*****` with the actually running pod name, then you will get a terminal inside the sirius pod. You will see a terminal at `/app/sirius` which contains the source code. The Nginx config is located at `/etc/nginx/`.

    You can also edit the sirius source code here locally. But this is **NOT** recommended because you will lose all the changes when the pod is terminated. (triggered by deployment)

---

### Manually deploy SIRIUS to Kubernetes

1. You should have Docker installed locally.

    https://docs.docker.com/install/

2. Checkout a github branch or commit

    `git checkout your_branch`

3. Build the Docker image

    Make sure you're in the SIRIUS root folder

    `cd SIRIUS-backend`

    Build the image with remote name

    `docker build -t us.gcr.io/valis-194104/sirius:YOUR_TAG .`

    Here `YOUR_TAG` should be changed to a unique tag you want, for exapmle you can use BRANCH_DATE like `FIX_0828`.

4. Test the newly built image locally

    a(1). If you have a local MongoDB instance running, you can run the docker container

    `docker run -ti -p 5000:5000 --link some-mongo:mongo us.gcr.io/valis-194104/sirius:YOUR_TAG bash`

    a(2). Else, if you don't have a local MongoDB instance, you can port-forward the cloud MongoDB

    `kubectl port-forward mongo-0 27017`

    b. Then run the docker container

    `docker run -ti -p 5000:5000 us.gcr.io/valis-194104/sirius:YOUR_TAG bash`

    c. Now you have a terminal at `/app/sirius` running inside the docker container, you can run the tests by

    `python -m unittest discover`

    d. If all tests passed, you can also manually launch the flask server

    `flask run -h 0.0.0.0`

    e. When the server is running, your webpack frontend launched by `npm run dev` will be able to connect to `localhost:5000`

5. Deploy to Kubernetes cloud server

    ** CAREFUL: This operation will affect other developers/users who might be using the cloud server.

    a. Push the Docker image to Google Cloud container registry

    `gcloud docker -- push us.gcr.io/valis-194104/sirius:YOUR_TAG`

    Here `YOUR_TAG` is the unique tag you previous used when building the image.

    b. Patch the deployment with the new image

    `kubectl patch deployment sirius-dev -p '{"spec":{"template":{"spec":{"containers":[{"name":"sirius-dev","image":"us.gcr.io/valis-194104/sirius:YOUR_TAG"}]}}}}'`

    Here `YOUR_TAG` is the unique tag you previous used when building the image.

    c. Check the deployment is successful

    `kubectl get pod`

    In the results, you should see the `sirius-pod-***` has been updated, with a very short `AGE`. You might also catch the old pod that is being terminated.

---

### Manually restart cloud SIRIUS/Mongo server

The simplest way to remove an old pod and start a new one is deleting the pod

`kubectl delete pod sirius-dev-****`

Here `sirius-dev-****` is the running pod name. The deployment will immediately create a new pod.

Same for the MongoDB pod

`kubectl delete pod mongo-0`

Note: During the booting up of the new pod, the service will be unavailable, so this operation is not recommended. The better way to start a new pod is deploy an update.

---

### Rebuild the MongoDB Database

The rebuilding of the cloud database is make very simple by the python script `rebuild_mongo_database.py`, but this operation will take quite some time to finish and should be very carefully done. There are in general two ways to do this.

#### Delete then rebuild the current database

**IMPORTANT** During the rebuilding of MongoDB database, the queries will result in empty or wrong results, and they will take very long time before the database index is built.

1. To prevent a deployement update from interupting the building process, we create a single pod

    `cd SIRIUS-backend/config/kubernetes/dev`

    `kubectl create -f single-pod.yaml`

    After this, you should see a new `sirius-pod` start running in `kubectl get pod` output.

2. Get a bash terminal on the new pod

    `kubectl exec -ti sirius-pod bash`

3. Set your MongoDB username and password with write permission

    `export MONGO_UNAME=USERNAME`

    `export MONGO_PWD=PASSWORD`

4. Goto the local cache folder

    `cd /cache`

    If the `gene_data_tmp` folder exists here, the downloaded datafiles inside it may be used to skip the downloading.

5. Run the rebuild script

    `/app/sirius/tools/rebuild_mongo_database`

    Note: Only 5 ENCODE bed files will be uploaded by default. To parse and upload the rest of the ENCODE bed files, you can go to `gene_data_tmp/ENCODE/` then run `/app/sirius/tools/automate_encode_upload.py -s 5 -e 2000`

6. Clean up

    Exit the `sirius-pod`, then delete it

    `kubectl delete pod sirius-pod`

#### Hot-swap the database

**Note:** The hot-swap is done by building a new database while the old one is up and running, then swap the backend to use the new database. This will prevent the service to be down during this time, but require more disk spaces and the precedure is more complicated.

Step 1-4 is the same as above.

5. Change the source code to use another database

    Edit `/app/sirius/mongo/__init__.py`, find the two lines

    ```
    db = client.database
    #db = client.database1
    ```

    Switch the `#` sign at start of line to the other one, e.g.

    ```
    #db = client.database
    db = client.database1
    ```

    Save this change. This will tell the rebuild script to use the other database.

6. Run the rebuild script in the `/cache` folder

    `/app/sirius/tools/rebuild_mongo_database`

    After this step, a new database (`database1`) is created.

7. Swap the SIRIUS server to use the new database

    a. Update the `sirius/mongo/__init__.py` in your local git repository the same way as above.

    b. Push this change to the master branch. Circle-CI should run the tests then deploy the update.

    After this step, the SIRIUS server should be connected to the newly built database.

8. Clean up

    a. Delete the old database

    Inside `sirius-pod`, run command to drop the old database

    `python -c "from sirius.mongo import client; client.drop_database('OLD_DATABASE')"`

    **CAREFUL** `'OLD_DATABASE'` should be the name of the old database. (Don't drop the new one!)

    b. Delete the `sirius-pod`

    `kubectl delete pod sirius-pod`

---

## For production server

**IMPORTANT** The production server is serving our website http://valis.bio. The operations on the production server should be done with extra care.

The production server was setup to use another namespace in Kubernetes. To check the status

`kubectl get pod --namespace production`

All other `kubectl` commands also work with the option `--namespace production`.

#### Deployment to the production server

The deployment to the production server is not configured in Circle-CI. To manually update the SIRIUS production server, first go to the root SIRIUS folder, then run the script:

`SIRIUS-backend$ ./config/kubernetes/production/build_and_deploy.sh`
