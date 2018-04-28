# -*- coding: utf-8 -*-
"""
Created on Thu Apr 19 22:50:42 2018

@author: arkil
"""
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.mllib.linalg import DenseVector
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import sql

sc =SparkContext("local","MachineLearning")
sqlContext =sql.SQLContext(sc)
rdd = sc.textFile('Social_Network_Ads.csv')
slicedRDD = rdd.map(lambda line :line.split(","))
df =slicedRDD.map(lambda line : Row(Index=line[0],User ID=line[1],Gender=line[2],Age=line[3],EstimatedSalary=line[4],Purchased=line[5])).toDF()
df =df.withColumn("Age",df["Age"].cast(FloatType()))
df =df.withColumn("EstimatedSalary",df["EstimatedSalary"].cast(FloatType()))
df =df.withColumn("Purchased",df["Purchased"].cast(IntegerType()))
df =df.drop("User ID")
df =df.drop("Index")
df =df.drop("Gender")
df =df.withColumn("EstimatedSalary",col("EstimatedSalary")/150000)
df =df.withColumn("Age",col("Age")/70)
df =df.select("Purchased","Age","EstimatedSalary")

inputData = df.rdd.map(lambda x:(x[0],DenseVector(x[1:])))
dataFrameSplit=inputData.toDF(["label","features"])
dataFrameSplit=dataFrameSplit.withColumn("label",dataFrameSplit["label"]).cast(DoubleType())

train,test =dataFrameSplit.randomSplit([.8,.2])
labelIndexer=StringIndexer(inputCol="label",outputCol="indexedLabel").fit(dataFrameSplit)
randomForestClassifier =RandomForestClassifier(labelCol="indexedLabel",featuresCol="features")
rfModel = randomForestClassifier.fit(labelIndexer.transform())
predictions = rfModel.transform(test)
evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel",predictionCol="prediction",metricName="precision")
accuracy = evaluator.evaluate(predictions)
print("Accuracy = %g" % (1.0-accuracy))


