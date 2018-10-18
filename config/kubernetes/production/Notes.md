# creating node pool for sirius with customized permission scope

gcloud container node-pools create pool-prod-sirius --cluster=cluster-sirius \
--num-nodes=1 --machine-type=n1-highmem-16 --local-ssd-count=1 \
--disk-size=100 --disk-type=pd-ssd \
--node-labels=sirius=prod \
--scopes=storage-full,compute-rw,cloud-source-repos,monitoring-write,service-control,cloud-platform

# creating node pool for mongo with customized permission scope

gcloud container node-pools create pool-prod-mongo --cluster=cluster-sirius \
--num-nodes=1 --machine-type=n1-highmem-32 \
--disk-size=100 --disk-type=pd-ssd \
--node-labels=sirius=prod