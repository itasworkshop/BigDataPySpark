from pyspark.sql import SparkSession

sparkSession = SparkSession.builder.getOrCreate()

#df = sparkSession.read.csv("./resources/deckofcards.txt",sep="|")
df = sparkSession.read.csv("./resources/largedeck.txt",sep="|")
#df.show()

print(df.count())

bdf = df.filter("_c0 = 'BLACK'").where("_c2 = 'Q'").collect()
#print(bdf)
print(len(bdf))