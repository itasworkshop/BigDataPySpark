from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DecimalType

sparkSession = SparkSession.builder.getOrCreate()

nyse_schema = StructType([
    StructField("stockticker", StringType(), True),
    StructField("tradingdate", StringType(), True),
    StructField("open_price", FloatType(),True),
    StructField("high_price", FloatType(), True),
    StructField("low_price", FloatType(), True),
    StructField("close_price", FloatType(), True),
    StructField("volume", IntegerType(), True)
])

company_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("lastsale", DecimalType(),True),
    StructField("marketcap", FloatType(), True),
    StructField("col_5", StringType(), True),
    StructField("col_6", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("url", StringType(), True)
])

nyse_df = sparkSession.read.csv("./resources/nyse_2009.csv",schema=nyse_schema)
company_df = sparkSession.read.csv("./resources/companylist_noheader.csv",schema=company_schema,sep="|")

nyse_df.printSchema()
company_df.printSchema()

print(nyse_df.count())
print(company_df.count())

nyse_df.createOrReplaceTempView("temp_nyse")
company_df.createOrReplaceTempView("temp_comp")
resultdf = sparkSession.sql("select n.stockticker,c.name,sum(n.volume) from temp_nyse n, temp_comp c where n.stockticker = c.symbol group by n.stockticker,c.name order by sum(n.volume) desc")

resultdf.show()
print(resultdf.count())