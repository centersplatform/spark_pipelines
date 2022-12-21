from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator


default_args = {
    'retries':2,
    'owner': 'hive',
}

#cmd1='pwd'
#cmd2='kubectl get pods -n airflow'
#change_hadoop_user_cmd= "export HADOOP_USER_NAME=hive"
access_spark_pod_cmd='kubectl exec -it spark-master-0 -n spark  -- '
spark_submit_cmd="""spark-submit --master spark://spark-master-svc:7077 --class org.data_training.App \
tmp/NTTData-1.0-SNAPSHOT.jar LoadDataToDW --executor-memory 10g --driver-memory 10g
"""

with DAG(
    dag_id='SSH_Operator_Load_Data_to_HIVE',
    default_args=default_args,
    start_date=datetime(2022, 12, 21),

) as dag:
    ssh_task = SSHOperator(
		        ssh_conn_id= 'ssh_default', 
		        task_id='ssh_submit_task', 
                command= access_spark_pod_cmd+spark_submit_cmd,
		        dag=dag
    )

ssh_task 