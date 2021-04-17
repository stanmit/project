from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("demo").getOrCreate()
dfi=spark.read.csv('/user/root/sqoop1/part-m-00000')
dfj=spark.read.csv('/user/root/sqoop2/part-m-00000')
df3=dfi.union(dfj)
df3=df3.drop('_c15')
df7=spark.read.parquet('s3://project-mahesh/raw_zone/nse70')
df=df7.union(df3)
df.coalesce(1).write.parquet('s3://project-mahesh/enriched')
