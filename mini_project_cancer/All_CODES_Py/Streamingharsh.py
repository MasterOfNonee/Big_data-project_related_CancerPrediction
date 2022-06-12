from pyspark import SparkContext, SparkConf, SQLContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

master = 'local'
appName = 'mini_project'

config = SparkConf().setAppName(appName).setMaster(master)

ss = SparkSession.builder.appName('MySparkStreamingSession')\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()

print(ss.sparkContext.getConf().get("spark.serializer"))
print('Done')

if ss:
    print(ss.sparkContext.appName)
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



CancerData = ss.readStream.schema(schema)\
    .option('header', True) \
    .csv('/home/harsh/Downloads/Streaming/')


query = CancerData.writeStream \
    .outputMode('append')\
    .option('header', True)\
    .option("checkpointLocation",'hdfs://localhost:9000/mini_project/ch')\
    .format('csv')\
    .option('path','hdfs://localhost:9000/mini_project/raw/stage').start()

query.awaitTermination()