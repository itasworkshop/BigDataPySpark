import pyspark
from pyspark.sql import SparkSession

sparkSession = SparkSession.builder.getOrCreate()

df = sparkSession.sql("select 'spark' as hello")
df.show()