from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 


default_args = {
    'retries':2,
    'owner': 'hive',
}

with DAG(
    dag_id='spark_submit_operator',
    default_args=default_args,
    start_date=datetime(2022, 12, 20),

) as dag:
    spark_submit_local = SparkSubmitOperator(
        		application ='/opt/bitnami/spark/tmp/NTTData-1.0-SNAPSHOT.jar',
                #application = '/tmp/test.py',
                java_class = "org.data_training.App",
		        conn_id= 'spark_default', 
		        task_id='spark_submit_task', 
                application_args=["LoadDataToDW"],#,"hdfs://192.168.182.17:8020/hive/warehouse/hive/warehouse/ecom.db/customers_dataset/customers_dataset.csv"],
		        dag=dag,
                executor_memory='8G',
                executor_cores='4'
    )


spark_submit_local