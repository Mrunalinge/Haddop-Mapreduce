# -*- coding: utf-8 -*-
"""DICTeammy1.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1fYKrHsMYM4Nmn3LLyBAvkCJHW5AHuiVc
"""

!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://archive.apache.org/dist/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
!tar xf spark-2.4.0-bin-hadoop2.7.tgz
!pip install -q findspark

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-2.4.0-bin-hadoop2.7"

import findspark
import pandas as pd
import numpy as np
findspark.init()
from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Matrix, Matrices

spark = SparkSession.builder.master("local[*]").getOrCreate()
#train_df = spark.read.csv('/content/train.csv', header = True)
pd_df = pd.read_csv(r'train.csv')
train_df = spark.createDataFrame(pd_df)
mapping_df = spark.read.csv('/content/mapping.csv', header = True)
test_dfi = spark.read.csv('/content/test.csv', header = True)

train_df.show()
mapping_df.show()
df1=train_df.select("genre")
#train_df.column("genre")
df1.show(5)

train_df.printSchema()

from pyspark.ml.feature import Tokenizer, RegexTokenizer


#plotToken = Tokenizer(inputCol="plot", outputCol="splitWords")
#plotToken.transform(dataset).head()
#dataset.na.drop(subset=["plot"])
regexToeknizer = RegexTokenizer(inputCol="plot", outputCol="tokens", pattern="\\W")
#dataset = regexToeknizer.transform(train_df)
#dataset.printSchema()
#dataset.select("plot").show(5)

from pyspark.ml.feature import StopWordsRemover
remover= StopWordsRemover(inputCol="tokens", outputCol="stopRemove")
#dataset=remover.transform(dataset)
#dataset.printSchema()
#dataset.show(2)

from pyspark.ml.feature import CountVectorizer,HashingTF
from pyspark.sql.functions import when, col, coalesce, array
from pyspark.ml import Pipeline

#fillNull = array().cast("array<string>") #solution for null handel from stackOverflow
#stopRemover = when(col("stopRemove").isNull(), fillNull).otherwise(col("stopRemove"))
#dataset.withColumn("stopRemove",stopRemover)

#hashingTF = HashingTF(inputCol="stopRemove", outputCol="features")
#result = hashingTF.transform(dataset)
#result.show(truncate=False)
cv = CountVectorizer(inputCol="stopRemove", outputCol="features")
#word2Vec = Word2Vec(inputCol="splitWords", outputCol="features", minCount=1)
pipeline = Pipeline(stages=[plotToken,remover, cv])

model = pipeline.fit(train_df)
result = model.transform(train_df)

model_test_dfi = pipeline.fit(test_dfi)
result_test_dfi = model.transform(test_dfi)

#cv = CountVectorizer(inputCol="stopRemove", outputCol="features")
#model = cv.fit(dataset)
#result = model.transform(dataset)
#result.show(truncate=False)

array  = [0]*20
print(array)
mvv = result.select("movie_name").rdd.flatMap(lambda x: x).collect()
print(mvv)
from pyspark.mllib.linalg import Matrix, Matrices

# Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
dm2 = Matrices.dense(3, 4, [1, 2, 3, 4, 5, 6,7,8,9,10,11,12])
dm3 = Matrices.dense(1, 12, [1, 2, 3, 4, 5, 6,7,8,9,10,11,12])
print(dm2)
print(dm3)
#dm4 = dm2+dm3
#print(dm4)

from pyspark.sql.functions import col
  mapping = {}
  #train_data, test_data = result.randomSplit([1.0,0.0], seed=100)
  train_data = result
  train_data.printSchema()
  #df_new = mapping_df.rename(columns={'_c0': 'A'})
  mapping_df1 = mapping_df.select(col("_c0").alias("indexes"),col("0").alias("genres"))
  mapping_df1.show()
  #print(range(mapping_df1.collect()))
  mapping = {}
  #print(range(mapping_df1['genres'].shape[0]))
  for i in mapping_df1.collect():
    genre = i["indexes"]
    #print(genre)
    mapping[i['genres']] = genre
print(mapping)
  ncols = mapping_df1.count()
  nrows = train_data.count()
  array  = [0]*nrows*ncols
  dm2 = Matrices.dense(nrows,ncols,array)
  print(dm2)
  #print(array)
  #y = np.zeros((df2['genre'].shape[0],len(mapping)))
  j=-1
  for i in train_data.collect():
    j=j+1
    genres = i["genre"]
    #print(genres)
    #if genres == 'None':
     # print(genres)
    try:
       #genres[0]
       if genres[0] == '[':
         #print(genres)
         genres = genres.replace('[','')
         genres = genres.replace('[','')
         genres = genres.replace(']','')
         genres = genres.replace('"','')
         genres = genres.replace("'","")
         genres = genres.split(sep = ',')
         r = 0
         for genre in genres:
           r = r + nrows
           genre = genre.strip()
           array[int(mapping[genre])+j+r] = 1
           #print(int(mapping[genre])+j)
           #print(genre)
    except TypeError:
        print("not able to print")
        print(genres)
  print(array)
  dm2 = Matrices.dense(nrows,ncols,array)
  print(dm2)



#array[20:][:]

rows = dm2.toArray().tolist()
df = spark.createDataFrame(rows,['col0','col1','col2','col3','col4','col5','col6','col7','col8','col9','col10','col11','col12','col13','col14','col15','col16','col17','col18','col19'])
df.show()

# Commented out IPython magic to ensure Python compatibility.
# %%time
# from pyspark.ml.classification import LogisticRegression
# from pyspark.sql.functions import col
# from pyspark.sql.functions import monotonically_increasing_id
# import pyspark.sql.functions as F
# from pyspark.sql.functions import concat_ws
# from pyspark.sql.types import StringType, IntegerType, ArrayType
# import pandas as pd
# from functools import reduce
# 
# 
# 
# predict= []
# 
# df = df.withColumn("id", monotonically_increasing_id())
# train_data = train_data.withColumn("id", monotonically_increasing_id())
# trainFinal = train_data.join(df,"id","outer").drop("id")
# trainFinal.show()
# trainFinal = trainFinal.filter(" COALESCE(movie_id, movie_name, plot, genre) IS NOT NULL")
# trainFinal = trainFinal.filter(" COALESCE( tokens, stopRemove) IS NOT NULL")
# trainFinal = trainFinal.filter(" COALESCE( features ) IS NOT NULL")
# trainFinal = trainFinal.na.drop()
# trainFinal = trainFinal.where(col("features").isNotNull())
# trainFinal = trainFinal.fillna(1)
# 
# rows1 = dm2.toArray()
# noRows= result_test_dfi.count()
# noCols = 20 
# #len(test_data.columns)
# arr_size = noRows * noCols 
# print(arr_size)
# testArr = [0]*arr_size
# #testMatrix = Matrices.dense(test_data.count(),len(test_data.columns),testArr)
# j=-1
# m=1
# 
# for i in mapping_df.collect():
#   j=j+1
#   col = "col"+str(j)
#   logReg = LogisticRegression(featuresCol="features", labelCol=col,family="multinomial")
#   logRegModel = logReg.fit(trainFinal)
#   print(str(logRegModel.coefficientMatrix))
#   predictions=logRegModel.transform(result_test_dfi)
#   predictions = predictions.withColumn("prediction", F.col("prediction").cast(IntegerType()))
#   predictions.select('prediction').show(10)
#   predict.append(predictions.select("movie_id","prediction"))
# 
# print(predict)
# 
#    
#   #summary = logRegModel.summary
#   
# 
# 
# #for i in mapping_df.collect():
# #logReg = LogisticRegression(featuresCol="features", labelCol=df["col1"],family="multinomial")
# #logRegModel = logReg.fit(train_data)
#   #  m=m+1
# #summary = logRegModel.summary
# #summary.show(5)
#  # j=j+1
#  # m=1
#   
# 
# #logReg = LogisticRegression(featuresCol="features", labelCol="genreIndex", family="multinomial")
# #logRegModel = logReg.fit(train_data)
# #summary = logRegModel.summary
# #summary.show(5)

# Commented out IPython magic to ensure Python compatibility.
# %%time
# from functools import reduce
# 
# predictNew = [df.selectExpr('movie_id', f'prediction as prediction_{i}') for i, df in enumerate(predict)]
# temp_df = reduce(lambda x, y: x.join(y, ['movie_id'], how='full'), predictNew)
# col_list = ['prediction_%d' % i for i in range(len(predict))]
# temp_df = temp_df.withColumn('predictions',concat_ws(" ",*col_list)).drop(*col_list).toPandas().to_csv("predictions_part1.csv",index=False)
# 
# 
# 
# 
# 
# 
# """from pyspark.ml.classification import LogisticRegression
# from pyspark.sql.types import *
# from pyspark.sql import SQLContext
# from pyspark import SparkContext
# import pyspark.sql 
# 
# sc = spark.sparkContext
#  
# sqlContext = SQLContext(sc)
# #y_test = np.zeros((?,20)) #---->give the size of y_test
# j=-20
# 
# #predict_train=logReg.transform(train_data)
# 
# """
# 
# 
# """schema = StructType([
#     StructField("predict", LongType(), True)
# ])
# df = sqlContext.createDataFrame(sc.emptyRDD(),schema)
# df.printSchema()
# for i in range(20):
#   j=j+20
#   print(i+j)
#   # fit column i (each individual genre)
#   logReg = LogisticRegression().fit(test_data,df)
#   
#   df = logReg.predict(test_data)"""