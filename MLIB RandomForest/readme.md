Classification using Spark Mlib

* RandomForest Classfier used for classification
* Use spark's computing power to work with large dataset
* Processto to run 
 
1) Copy the dataset file to hdfs :

hdfs dfs -put Social_Network_Ads.csv /home/user/project

2) Use Pyspark shell to run Individual commands or Run file
spark-submit RanodmForestClassificationMlib.py or
pyspark RanodmForestClassificationMlib.py
 
 