import os

#import findspark
from pyspark import SparkContext, SparkConf, SQLContext, HiveContext
from pyspark.sql import SparkSession

#os.environ['SPARK_HOME'] = '/home/abhay/MyHome/InstallHome/Apache/spark'

# findspark.init()

master = 'local'
appName = 'PySpark_Dataframe Hive Operations'



warehouse_location = 'hdfs://localhost:9000/user/hive/warehouse'
metastore_location = '/home/harsh/DBDA_HOME/apache-hive-2.3.9-bin'


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("spark.sql.catalogImplementation", "hive") \
    .config('hive.metastore.warehouse.dir', metastore_location) \
    .enableHiveSupport() \
    .getOrCreate()

if SparkSession.sparkContext:
    print('===============')
    print(f'AppName: {spark.sparkContext.appName}')
    print(f'Master: {spark.sparkContext.master}')
    print('===============')
else:
    print('Could not initialise pyspark session')

spark.sql("CREATE DATABASE IF NOT EXISTS mini_project1")
spark.sql("use mini_project1;")
spark.sql("SHOW DATABASES").show()
spark.sql(
        "create external table if not exists staging1 (Clump Integer , UniCell_Size Integer, Uni_CellShape Integer, MargAdh Integer, SEpith Integer, BareN Integer,Bchromatin  Integer, NoemN Integer, Mitoses Integer ,Class String) stored as parquet location '/mini_project/raw/stage/persist';")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;")

spark.sql(
        "create external table if not exists Cancer_partitioned (Clump Integer , UniCell_Size Integer, Uni_CellShape Integer, MargAdh Integer, SEpith Integer, BareN Integer, Bchromatin  Integer, NoemN Integer, Mitoses Integer) PARTITIONED BY (Class String) stored as parquet location '/mini_project/raw/partition';")

spark.sql(
        "insert overwrite table Cancer_partitioned partition(Class) select Clump, UniCell_Size, Uni_CellShape, MargAdh, SEpith, BareN, Bchromatin, NoemN, Mitoses, Class from staging1;")
spark.sql("SHOW TABLES").show()
spark.sql("CREATE DATABASE IF NOT EXISTS report")
spark.sql("use report;")
spark.sql(
        "create table if not exists report.Malignant_count as select count(*) from mini_project1.Cancer_partitioned where Class='Malignant';")
spark.sql(
        "create table if not exists report.Benign_count1 as select count(*) from mini_project1.Cancer_partitioned where Class='Benign';")

spark.sql("select * from report.Benign_count1;").show()
spark.sql("select * from report.Malignant_count;").show()


