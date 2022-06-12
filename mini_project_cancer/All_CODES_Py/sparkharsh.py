from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

master = 'local'
appName = 'Mini_project Persist'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)

sqlContext = SQLContext(sc)
ss = SparkSession(sc)


if ss:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')



schema = StructType([StructField('Code',IntegerType(), True),
                            StructField('Clump', IntegerType(), True),
                            StructField('UniCell_Size', IntegerType(), True),
                            StructField('Uni_CellShape', IntegerType(), True),
                            StructField('MargAdh', IntegerType(), True),
                            StructField('SEpith', IntegerType(), True),
                            StructField('BareN', IntegerType(), True),
                            StructField('BChromatin', IntegerType(), True),
                            StructField('NoemN', IntegerType(), True),
                            StructField('Mitoses', IntegerType(), True),
                            StructField('Class', StringType(), True)])

df=ss.read.csv('hdfs://localhost:9000/mini_project/raw/stage/dq_good',schema=schema)
print(df.columns)
df1=df.drop('Code')


df1.write.option('header',False).mode('overwrite').parquet('hdfs://localhost:9000/mini_project/raw/stage/persist')

