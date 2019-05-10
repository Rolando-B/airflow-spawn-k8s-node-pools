from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.contrib.kubernetes.pod import Resources
from airflow.models import Variable

# USEFUL VARIABLES
gcloud_image = Variable.get("gcloud_image")
image = Variable.get("pod_image")
cluster_name = Variable.get("cluster_name")
service_account_email = Variable.get("service_account_email")


_MAIN_DAG_ID = "test-dag-spawn-pools"
pool_name = "test-dag-spawn-pool"
machine_type = "n1-standard-4"
num_nodes = 1
node_taint = "only-test-pod"

pod_resources = Resources(request_memory = "10000Mi",
	                      limit_memory = "10000Mi",
                          limit_cpu = "2",
                          request_cpu = "2")

default_args = {
    "owner":               "airflow",
    "start_date":          datetime(2018, 12, 1),
    "email":               ["fake-email@fake.io"],
    # Define some variables to cancel email sending
    "email_on_failure":    False,
    "email_on_retry":      False,
    "retries":             3,
    "retry_delay":         timedelta(seconds=5),
    'provide_context':     True,
    'catchup':             True
}

dag = DAG(
    _MAIN_DAG_ID,
    description=f"Example DAG for node-pool creation",
    default_args=default_args,
    catchup=True,
    schedule_interval=None,
)

with dag :
    k8s_pool_create = kubernetes_pod_operator.KubernetesPodOperator(
            task_id='create_node_pool_k8s',
            name=f'create-{pool_name}',
            namespace="default",
            image=gcloud_image,
            image_pull_policy='Always',
            cmds=['bash'],
            get_logs=True,
            arguments=[
                '-c',
                'function wait_for_operation_completed(){\n'
                'OPERATION_NAME=$(gcloud container operations list'
                ' --filter="(TYPE:CREATE_NODE_POOL OR TYPE:DELETE_NODE_POOL) AND STATUS:RUNNING"' 
                ' --format="value(NAME)"); '
                'if [ -z "$OPERATION_NAME" ];\n'                                         
                ' then\n'
                ' echo "No operation running, can create node-pool"\n'
                ' else\n'
                ' gcloud container operations wait $OPERATION_NAME' 
                ' --region=europe-west1-b\n'
                ' fi\n'
                'READY=1\n'
                '}\n'
                'function create_node_pool(){\n'
                f'gcloud container node-pools create {pool_name}' 
                f' --cluster {cluster_name}'
                ' --disk-type pd-ssd'
                ' --enable-autorepair'
                ' --enable-autoupgrade'
                f' --machine-type {machine_type}'
                ' --metadata disable-legacy-endpoints=true'
                f' --num-nodes {num_nodes}'
                ' --zone europe-west1-b'
                f' --node-taints dedicated={node_taint}:NoSchedule'
                f' --node-labels name={pool_name}'
                f' --service-account {service_account_email};'
                '}\n'
                'READY=0\n'
                'while [ $READY -ne "1" ]\n'
                'do\n'
                '      wait_for_operation_completed\n'
                '      create_node_pool || READY=0\n'
                'done'
            ],
            #in_cluster=True # Then Airflow can find the right Kube config in the cluster
        )

    k8s_pod = kubernetes_pod_operator.KubernetesPodOperator(
            task_id='pod_execution',
            name="pod-ex-minimum",
            namespace="default",
            image=image,
            image_pull_policy='Always',
            resources=pod_resources,
            is_delete_operator_pod=True,
            node_selectors={"name": f"{pool_name}" },
            tolerations=[{'key': 'dedicated',
                          'operator': 'Equal',
                          'value': f"{node_taint}",
                          'effect': "NoSchedule" }],
            cmds=['bash'],

            arguments=[
                '-c',
                'echo "hello world"'],
            #in_cluster=True # Then Airflow can find the right Kube config in the cluster
        )

    k8s_pool_delete = kubernetes_pod_operator.KubernetesPodOperator(
            task_id='delete_pool_k8s',
            name=f'delete-{pool_name}',
            namespace="default",
            image=gcloud_image,
            is_delete_operator_pod=True,
            image_pull_policy='Always',
            cmds=['bash'],
            arguments=[
                '-c',
                'function wait_for_operation_completed(){\n'
                'OPERATION_NAME=$(gcloud container operations list'
                ' --filter="(TYPE:CREATE_NODE_POOL OR TYPE:DELETE_NODE_POOL) AND STATUS:RUNNING"' 
                ' --format="value(NAME)"); '
                'if [ -z "$OPERATION_NAME" ];\n'                                         
                ' then\n'
                ' echo "No operation running, can delete node-pool"\n'
                ' else\n'
                ' gcloud container operations wait $OPERATION_NAME' 
                ' --region=europe-west1-b\n'
                ' fi\n'
                'READY=1\n'
                '}\n'
                'function delete_node_pool(){\n'
                f'gcloud container node-pools delete {pool_name}' 
                f' --cluster {cluster_name}'
                ' --zone europe-west1-b'
                ' --quiet;'
                '}\n'
                'READY=0\n'
                'while [ $READY -ne "1" ]\n'
                'do\n'
                '      wait_for_operation_completed\n'
                '      delete_node_pool || READY=0\n'
                'done'
            ],
            #in_cluster=True # Then Airflow can find the right Kube config in the cluster
    )


k8s_pool_create >> k8s_pod >> k8s_pool_delete