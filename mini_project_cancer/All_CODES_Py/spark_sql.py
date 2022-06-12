from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, max, sum

master = 'local'
appName = 'PySpark_Data format Operations'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)
# You will need to create the sqlContext for SparkSQL
sqlContext = SQLContext(sc)
# You will need to create the SparkSession for streaming
ss = SparkSession(sc)

if ss:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

print("ABOVE  WE CREATED THE SPARK CONTEXT , FOR RUNNING  NEW PROGRAM WE CREATED THE SPARK CONTEXT")
print("---------------------------------------------------------------------------------------------------")
df=ss.read.parquet("hdfs://localhost:9000/mini_project/raw/stage/persist/")
df.show()
Report1=df.groupBy(['Class']).count()

Report1.write.mode('overwrite').parquet('hdfs://localhost:9000/mini_project/sparkreport1')
#below commadn is added jut hsow output inn pychram, I had commented them during airflow runtime..
Report1.show()
Report2=df.groupBy(['UniCell_Size']).count()
Report2.write.option('header',False).mode('overwrite').parquet('hdfs://localhost:9000/mini_project/sparkreport2/')
#below commadn is added jut hsow output inn pychram, I had commented them during airflow runtime..
Report2.show()
Report3=df.filter(df['UniCell_Size'] > 8).select(['Clump', 'BareN', 'Class'])

Report3.write.option('header',False).mode('overwrite').parquet('hdfs://localhost:9000/mini_project/sparkreport3/')
#below commadn is added jut hsow output inn pychram, I had commented them during airflow runtime..
Report3.show()