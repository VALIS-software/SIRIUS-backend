# Workflow for setting up SIRIUS backend

We aim to provide an end-to-end work flow that will set up two Docker containers locally, one running SIRIUS, and one running MongoDB.

The following work flow has been tested on Ubuntu 16.04, and also the Linux subsystem in Windows 10 running Ubuntu.

## 1. Download SIRIUS

``` 
git clone git@github.com:VALIS-software/SIRIUS-backend.git
```

## 2. Install Docker if you don't have it yet

https://www.docker.com/get-docker

## 3. Setup local MongoDB server as a Docker container

### 3.1. Download and launch local mongo docker image.

```
sudo docker run -p 27017:27017 --name some-mongo -d mongo --auth
``` 

This will download the official `mongo` Docker image and run it in detached mode with auth turned on.

### 3.2. Setup initial admin user in local MongoDB.

#### 3.2.1 Launch a bash shell inside mongo container

```
sudo docker exec -it some-mongo bash
```

you should get a bash terminal inside the container, like 

```
root@093275de44e7:/#
```

#### 3.2.2 Run initial mongo shell

```
mongo admin
```

you should get a mongo shell like

```
(some message)
> 
```

#### 3.2.3 Create initial admin user

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

### 3.3 Setup user for sirius

#### 3.3.1 First exit the current mongo shell

```
> exit
```

#### 3.3.2 Login back again with the just-created admin user

```
mongo -u admin -p password --authenticationDatabase admin
```

If successful, you should get a new mongo shell like 

```
(some message)
>
```

#### 3.3.3 Switch to a new database called `testdb`

```
> use testdb
```

#### 3.3.4 Create the user `sirius` in testdb

```
db.createUser({user: 'sirius', pwd: 'valis', roles:[{role: 'readWrite', db: 'testdb'}, {role: 'readWrite', db:'database'}, {role: 'readWrite', db:'database1'}] })
```

After this step, you should see another `Successfully added user` message, and your MongoDB container is ready to go!

Note: Here the MongoDB does not have any data yet. We will first run SIRIUS in a docker image in the next step, then upload data in there. 



## 4. Build Docker image for SIRIUS and launch bash shell inside

### 4.1 Build the SIRIUS Docker image:

```
sudo docker build -t sirius-dev:latest .
```

NOTE: This step may take a while to complete as several packages need to be downloaded.

Once the build is completed you should see a success message like this:

```
Successfully built 3d65f2920032
Successfully tagged sirius-dev:latest
```

### 4.2 Launch a bash terminal inside container:

```
sudo docker run -it --link some-mongo:mongo sirius-dev:latest bash
```

If successful, you should be given a bash shell like

```
root@09cee5403d0b:/app/sirius#
```


## 5. Download Genetic Data, parse and upload to local MongoDB server

Assuming you have finished step 4, so you're running bash shell inside the SIRIUS docker image.

### 5.1 Download the currently avaiable data files.

#### 5.1.1 Create a folder to put the data files

```
mkdir genome_data; cd genome_data
```

#### 5.1.2 Call the download script

```
/app/sirius/tools/download_genome_data.sh
```

This will print some messages showing the progress, and finish with 3 new folders `GRCh38_gff`, `gwas` and `eQTL` in the current folder.


### 5.2 Parse and upload data to local MongoDB server

#### 5.2.1 Parse and upload the GRCh38_gff data

Assuming the `SIRIUS-backend` repository is cloned into the home folder.

```
cd GRCh38_gff
/app/sirius/tools/parse_upload_gff_chunk.py GRCh38_latest_genomic.gff --upload
cd ..
```

The python script will print the progress of data parsing, and upload the parsed data to the local MongoDB server.

Some warning messages will be shown when parsing data, feel free to ignore them.

#### 5.2.2 Parse and upload the gwas data

```
cd gwas
/app/sirius/tools/parse_upload_data.py gwas.tsv gwas --upload
cd ..
```

Some warning messages will be shown when parsing data, feel free to ignore them.

#### 5.2.3 Parse and upload the eQTL data

```
cd eQTL
/app/sirius/tools/parse_upload_data.py GSexSNP_allc_allp_ld8.txt eqtl --upload
cd ..
```

#### 5.2.4 Build the indices in MongoDB

```
/app/sirius/tools/build_mongo_index.py
```

Now your local MongoDB server has all the data ready. You can then exit the Docker container

```
exit
```


## 6. Launch sirius in detached mode.

If there is an update to the SIRIUS code. You can build a new Docker image

```
sudo docker build -t sirius-dev:latest .
``` 

```
sudo docker run -p 5000:5000 --link some-mongo:mongo -d sirius-dev:latest
```

The Docker image has been configured to launch Nginx and uWSGI with Flask to serve the SIRIUS server at port 5000.

## 7. Run VALIS frontend in dev mode

At this point, you can run valis in dev mode

```
npm run dev
```

And it should be able to connect to the SIRIUS server.









