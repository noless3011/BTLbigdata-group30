from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from kubernetes.client import models as k8s

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'batch_layer_pipeline',
    default_args=default_args,
    description='Orchestrate Spark Batch Layer Jobs',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['batch', 'spark'],
) as dag:

    # Common configuration for all pods
    pod_config = {
        'namespace': 'default',
        'image': 'batch-layer:latest',
        'image_pull_policy': 'Never',  # Use local image in Minikube
        'service_account_name': 'default', # Or specific SA if needed
        'get_logs': True,
        'in_cluster': True,
        'is_delete_operator_pod': False,
        'env_vars': {
            'MINIO_ENDPOINT': 'http://minio.minio.svc:9000',
            'MINIO_ACCESS_KEY': 'minioadmin',
            'MINIO_SECRET_KEY': 'minioadmin',
            'CASSANDRA_HOST': 'cassandra'
        },
        'container_resources': k8s.V1ResourceRequirements(
            limits={
                'memory': '2Gi',
                'cpu': '1000m'
            }
        )
    }

    # Helper function to create task
    def create_batch_task(job_name):
        return KubernetesPodOperator(
            task_id=f'{job_name}_batch_job',
            name=f'{job_name}-batch-job',
            cmds=["python", "batch_layer/run_batch_jobs.py", job_name, "s3a://bucket-0/master_dataset", "s3a://bucket-0/batch_views"],
            **pod_config
        )

    # Define tasks
    auth_job = create_batch_task('auth')
    assessment_job = create_batch_task('assessment')
    video_job = create_batch_task('video')
    course_job = create_batch_task('course')
    profile_job = create_batch_task('profile_notification')

    # All jobs run in parallel (independent of each other)
    # In Airflow, simply defining them in the DAG context adds them to the DAG.
    # No dependencies need to be set since they are parallel.
