from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("demo").getOrCreate()
df10=spark.read.parquet('s3://project-mahesh/raw_zone/nse10')
df10.coalesce(1).write.saveAsTable('nse10')
