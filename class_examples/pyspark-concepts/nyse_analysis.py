from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

sparkSession = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("stockticker", StringType(), True),
    StructField("tradingdate", StringType(), True),
    StructField("open_price", FloatType(),True),
    StructField("high_price", FloatType(), True),
    StructField("low_price", FloatType(), True),
    StructField("close_price", FloatType(), True),
    StructField("volume", IntegerType(), True)
])

df = sparkSession.read.csv("./resources/nyse_2009.csv",schema=schema)
df.printSchema()

print(df.count())
df.createOrReplaceTempView("temp_nyse")
#resultdf = sparkSession.sql("select stockticker,count(*) from temp_nyse group by stockticker")

#resultdf = sparkSession.sql("select stockticker,sum(volume) from temp_nyse group by stockticker order by sum(volume) desc")
resultdf = sparkSession.sql("select stockticker,sum(volume) from temp_nyse group by stockticker order by sum(volume) desc limit 5")
resultdf.show()
print(resultdf.count())