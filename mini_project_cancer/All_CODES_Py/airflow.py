from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

args = {
    'owner':'HARSH'
}

dag = DAG(
    dag_id='Airflow_cancer',
    default_args=args,
    start_date=days_ago(1),
    schedule_interval=None,
    params={},
    tags=['edbda', 'cancer', 'project']
)

sparkPersist = SparkSubmitOperator(
    task_id='Spark-task',
    name="airflow-spark",
    application='/home/harsh/DBDA_HOME/DBDA_CODE/SPARK/pythonProject1/miniproject/sparkharsh.py',
    application_args=['1'],
    env_vars={'PYSPARK_DRIVER_PYTHON':'python',
              'HADOOP_CONF_DIR':'/home/harsh/DBDA_HOME/hadoop-3.3.1/etc/hadoop',
              'PYSPARK_PYTHON':'/usr/bin/python3'},
    dag=dag
)


Spark_query = BashOperator(task_id="hive_partitioningg",
                     bash_command="spark-submit --master local spark_sql.py",
                     cwd="/home/harsh/DBDA_HOME/DBDA_CODE/SPARK/pythonProject1/miniproject",
                     dag=dag)


hive_partitioning= BashOperator(task_id="Spark_queryy",
                     bash_command="spark-submit --master yarn Spark_hive.py",
                     cwd="/home/harsh/DBDA_HOME/DBDA_CODE/SPARK/pythonProject1/miniproject",
                     dag=dag)


BigDATA_ML= BashOperator(task_id="machine_learaning",
                     bash_command="spark-submit --master local CANCER_ml.py ",
                     cwd="/home/harsh/DBDA_HOME/DBDA_CODE/SPARK/pythonProject1/miniproject",
                     dag=dag)


Mapreduce = BashOperator(
    task_id='Map_reduce',
    start_date=days_ago(1),
    bash_command='hadoop jar /home/harsh/Downloads/mini_project/Mini_project/target/Mini_project-0.0.1-SNAPSHOT.jar  mapreduce.Mapreduce_job /mini_project/raw/stage/part-00000-3d3b2ea9-d88e-4dea-b974-2d6b512bcd9f-c000.csv /mini_project/raw/stage/dq_goody',
    params=None,
    default_args={},
    dag=dag
)



Mapreduce >> sparkPersist >> hive_partitioning >> Spark_query
Mapreduce >> BigDATA_ML
