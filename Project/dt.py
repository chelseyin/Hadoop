import numpy as py
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import Normalizer
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import lit
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.classification import NaiveBayes
from pyspark.sql import HiveContext

df = spark.read.format("csv").option("header", "true").load("chicago/data/food-inspections-new.csv")

df=df.na.drop()

df=df.withColumn("Latitude",df["Latitude"].cast(DoubleType()))
df=df.withColumn("Longitude",df["Longitude"].cast(DoubleType()))

assembler = VectorAssembler(inputCols=["Latitude","Longitude"],outputCol="assemfeatures")

normalizer = Normalizer(inputCol="assemfeatures", outputCol="features", p=1.0)

df_asse = assembler.transform(df)
df_norm = normalizer.transform(df_asse)

df=df_norm

featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(df)

dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

indexer = StringIndexer(inputCol="Risk", outputCol="label")
df = indexer.fit(df).transform(df)

labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(df)

pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

(trainingData, testData) = df.randomSplit([0.1, 0.9]) 
dtmodel = pipeline.fit(trainingData)
dtpredictions = dtmodel.transform(testData)     
dtevaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
dtaccuracy = dtevaluator.evaluate(dtpredictions)
print("Test Error = %g " % (1.0 - dtaccuracy))
print(dtaccuracy)
