# Databricks notebook source
# MAGIC %md
# MAGIC # Salting:
# MAGIC - Salting is a technique used to resolve data skewness
# MAGIC - we add some random value to the key column so that it will make the data to be distributed in a better way, while joining we will be using the added random key to join the data
# MAGIC - hash(key_coulmn)% no_of_shuffle_partititons
# MAGIC - if we add salting techinque it will be like
# MAGIC - hash(key_column, salting_key)%no_of_shuffle_partitions
# MAGIC - In this case it makes data more evenly distributed 
# MAGIC - eg keys: customer_id
# MAGIC

# COMMAND ----------

spark

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 3)
spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.set('spark.sql.adaptive.enabled', False)
spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df_uniform = spark.createDataFrame([i for i in range(1000000)], IntegerType())
df_uniform.show(5, False)

# COMMAND ----------

(
  df_uniform.withColumn("partition_id", spark_partition_id())
  .groupby("partition_id")
  .count()
  .orderBy("partition_id").show()
)

# COMMAND ----------

df0 = spark.createDataFrame([0] * 999990, IntegerType()).repartition(1)
df1 = spark.createDataFrame([1] * 15, IntegerType()).repartition(1)
df2 = spark.createDataFrame([2] * 10, IntegerType()).repartition(1)
df3 = spark.createDataFrame([3] * 5, IntegerType()).repartition(1)
df_skew = df0.union(df1).union(df2).union(df3)
df_skew.show(5, False)

# COMMAND ----------

(
  df_skew.withColumn("partition_id", spark_partition_id())
  .groupBy("partition_id")
  .count()
  .orderBy("partition_id")
  .show()
)

# COMMAND ----------

df_joined_c1 = df_skew.join(df_uniform, "value", 'inner')

# COMMAND ----------

(
  df_joined_c1.withColumn("partition_id", spark_partition_id())
  .groupBy("partition_id")
  .count()
  .orderBy("partition_id")
  .show()
)

# COMMAND ----------

SALT_NUMBER = int(spark.conf.get("spark.sql.shuffle.partitions"))
SALT_NUMBER

# COMMAND ----------

df_skew = df_skew.withColumn("salt", (rand()*SALT_NUMBER).cast("int"))

# COMMAND ----------

df_skew.display()

# COMMAND ----------

df_uniform = (
    df_uniform
    .withColumn("salt_values", array([lit(i) for i in range(SALT_NUMBER)]))
    .withColumn("salt", explode(col("salt_values")))
)

# COMMAND ----------

df_uniform.show(10, truncate=False)

# COMMAND ----------

df_joined = df_skew.join(df_uniform, ["value", "salt"], 'inner')

# COMMAND ----------

(
    df_joined
    .withColumn("partition", spark_partition_id())
    .groupBy("value", "partition")
    .count()
    .orderBy("value", "partition")
    .show()
)

# COMMAND ----------


