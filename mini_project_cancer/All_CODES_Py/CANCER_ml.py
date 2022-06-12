from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation, Summarizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.ml import Pipeline

appName = "Breast_cancer"
sparkSession = SparkSession.builder.master("local").appName(appName).getOrCreate()

if SparkSession.sparkContext:
    print('===============')
    print(f'AppName: {sparkSession.sparkContext.appName}')
    print(f'Master: {sparkSession.sparkContext.master}')
    print('===============')
else:
    print('Could not initialise pyspark session')

Cancer_Data1 = sparkSession.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", ",") \
    .csv("hdfs://localhost:9000/mini_project/raw/stage/part-00000-3d3b2ea9-d88e-4dea-b974-2d6b512bcd9f-c000.csv").drop('Code') \
    .withColumn("Class",
        when(col("Class") == "Malignant", 0)
        .when(col("Class") == "Benign", 1)
        .otherwise(3))
Cancer_Data1.show()


assembler = VectorAssembler(inputCols=["Clump","UniCell_Size","Uni_CellShape","MargAdh","SEpith","BareN","BChromatin","NoemN","Mitoses"],
                            outputCol="features").setHandleInvalid("skip")
#Cancer_vector= assembler.transform(Cancer_Data1)

randomForest = RandomForestClassifier(labelCol="Class", featuresCol="features",
                                      maxDepth=5, numTrees=50, seed=2022)

pipeline = Pipeline(stages=[assembler, randomForest])
# This is the place where we create our model
CancerPredictionModel = pipeline.fit(Cancer_Data1)

Cancer_TEST_Data1 = sparkSession.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", ",") \
    .csv("file:///home/harsh/Downloads/BreastCancer (TestTTT).csv")
prediction = CancerPredictionModel.transform(Cancer_TEST_Data1)

prediction.show()