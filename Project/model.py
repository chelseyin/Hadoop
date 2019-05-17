import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

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

# DecisionTree

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

(trainingData, testData) = df.randomSplit([0.5, 0.5]) 
dtmodel = pipeline.fit(trainingData)
dtpredictions = dtmodel.transform(testData)     
dtevaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
dtaccuracy = dtevaluator.evaluate(dtpredictions)
dt=['DecisionTree',str(dtaccuracy)]
print(','.join(dt))

# LogisticRegression

df = spark.read.format("csv").option("header", "true").load("chicago/data/food-inspections-new.csv")

df=df.na.drop()

df=df.withColumn("Latitude",df["Latitude"].cast(DoubleType()))
df=df.withColumn("Longitude",df["Longitude"].cast(DoubleType()))

assembler = VectorAssembler(inputCols=["Latitude","Longitude"],outputCol="assemfeatures")

normalizer = Normalizer(inputCol="assemfeatures", outputCol="features", p=1.0)

df_asse = assembler.transform(df)
df_norm = normalizer.transform(df_asse)

df=df_norm

indexer = StringIndexer(inputCol="Risk", outputCol="label")
df = indexer.fit(df).transform(df)

dfff=df.select(["label","features"])

lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
train,test = dfff.randomSplit([0.5, 0.5],0L)

lrModel = lr.fit(train)
lrpredictions = lrModel.transform(test) 
lrevaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
lraccuracy = lrevaluator.evaluate(lrpredictions)
lr=['LogisticRegression',str(lraccuracy)]
print(','.join(lr))

# RandomForest

df = spark.read.format("csv").option("header", "true").load("chicago/data/food-inspections-new.csv")

df=df.na.drop()

df=df.withColumn("Latitude",df["Latitude"].cast(DoubleType()))
df=df.withColumn("Longitude",df["Longitude"].cast(DoubleType()))

assembler = VectorAssembler(inputCols=["Latitude","Longitude"],outputCol="assemfeatures")

normalizer = Normalizer(inputCol="assemfeatures", outputCol="features", p=1.0)

df_asse = assembler.transform(df)
df_norm = normalizer.transform(df_asse)

df=df_norm

indexer = StringIndexer(inputCol="Risk", outputCol="label")
df = indexer.fit(df).transform(df)

labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(df)

featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(df)

(trainingData, testData) = df.randomSplit([0.5, 0.5]) 

rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10)

labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",labels=labelIndexer.labels)

rfpipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])

rfmodel = rfpipeline.fit(trainingData)
rfpredictions = rfmodel.transform(testData)
rfevaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
rfaccuracy = rfevaluator.evaluate(rfpredictions)
rf=['RandomForest',str(rfaccuracy)]
print(','.join(rf))
