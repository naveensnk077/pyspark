# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df0 = spark.createDataFrame([0] * 999990, IntegerType()).repartition(1)
df1 = spark.createDataFrame([1] * 15, IntegerType()).repartition(1)
df2 = spark.createDataFrame([2] * 10, IntegerType()).repartition(1)
df3 = spark.createDataFrame([3] * 5, IntegerType()).repartition(1)
df_skew = df0.union(df1).union(df2).union(df3)
df_skew.show(5, False)

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "3")
spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.set("spark.sql.adaptive.enabled", "false")

# COMMAND ----------

SALT_NUMBER = int(spark.conf.get("spark.sql.shuffle.partitions"))
SALT_NUMBER

# COMMAND ----------

(
    df_skew
    .withColumn("salt", (rand() * SALT_NUMBER).cast("int"))
    .groupBy("value", "salt")
    .agg(count("value").alias("count"))
    .groupBy("value")
    .agg(sum("count").alias("count"))
    .show()
)


# COMMAND ----------


