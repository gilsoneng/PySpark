# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 13:49:30 2020

@author: Gilson
"""

import pandas as pd
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

df2 = spark.read.option("delimiter", " ").csv("access_log_Jul95", header=False)

df3 = spark.read.option("delimiter", " ").csv("access_log_Aug95", header=False)

df = df2.union(df3)

df = df.selectExpr("_c0 as Host", "_c3 as Date", "_c6 as Error", "_c7 as Bytes")

df = df.withColumn("Bytes", df["Bytes"].cast(IntegerType()))

# Número de Hosts Unicos
ndistinct = df.select("Host").distinct().count()
print(f"Número de Hosts Únicos: {ndistinct}")

# Total de Erros 404
totalerror404 = df.filter(df['Error'] == 404).count()
print(f"Total de Erros 404: {totalerror404}")

# Os 5 URLs que mais causaram erro 404.
rank = df.filter(df['Error'] == 404).cube("Host").count().orderBy("count",ascending=False)
rank.show(5)

# Quantidade de erros 404 por dia
df.createOrReplaceTempView("view")
df1 = spark.sql("Select Host, LEFT(Replace(Replace( Date, '[', ''),'/', '-'),11) as Date, Error, Bytes from view")
df1.createOrReplaceTempView("days")
df1 = df1.withColumn("Bytes", df1["Bytes"].cast(IntegerType()))
COUNTDAYS = spark.sql("Select COUNT(distinct Date) as counter from days").toPandas()["counter"][0]
AVG404 = totalerror404/COUNTDAYS
print("Quantidade de erros 404 por dia {:.2f}".format(AVG404))

# O total de bytes retornados.
qtddbytes = spark.sql("Select SUM( Bytes ) as contador from days").toPandas()["contador"][0]
print(f"O total de bytes retornados: {qtddbytes} bytes")