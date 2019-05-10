# Work with node pools in Airflow

This short example shows the how to do 3 tasks in Airflow :

- A node pool creation
- A run of a pod inside that node-pool
- A node pool deletion

All the point of doing this is explained in this article :

## Adaptations needed

The code won't work in this state, you need to add these variables in Airflow :
- `gcloud_image` = The Google Cloud SDK docker image (https://hub.docker.com/r/google/cloud-sdk/)
- `pod_image` = An image where you want to execute something (can be the gcloud image)
- `cluster_name` = The name of the cluster you want to spawn a node pool into
- `service_account_email` = The email of the service account that can spawn pools (I have no idea why the node pool creation is asking this parameter, it should be able to do it without it) 

Also, you will need to adapt your `kubeconfig` if not done yet. 
In this example, I am using the default `kubeconfig` location (i.e `/usr/local/airflow/.kube/config`) because I ran Airflow in local. 
You can uncomment the line `in_cluster=True` if you are running Airflow on a k8s cluster, hence you won't need to manage the `kubeconfig` 

