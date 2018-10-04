# creating node pool with customized permission scope

gcloud container node-pools create pool-dev-1 --cluster=cluster-sirius \
--num-nodes=2 --machine-type=n1-highmem-8 --local-ssd-count=1 \
--disk-size=100 --disk-type=pd-ssd \
--node-labels=sirius=dev \
--scopes=storage-full,compute-rw,cloud-source-repos,monitoring-write,service-control,cloud-platform