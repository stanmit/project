from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("demo").getOrCreate()
from pyspark.sql.functions import monotonically_increasing_id
df=spark.read.csv('s3://project-mahesh/steps/fobhav.csv',header=True).limit(100)
df = df.drop("Unnamed: 15")
df70,df30=df.randomSplit([0.7,0.3],42)
df30=df30.withColumn("id", monotonically_increasing_id())
df70.coalesce(1).write.parquet('s3://project-mahesh/raw_zone/nse70')
df30.createOrReplaceTempView('df30')
df20=spark.sql('select * from df30 where id < (select (max(id)) from df30)/100*70')
df10=spark.sql('select * from df30 where id >= (select (max(id)) from df30)/100*70')
df10.coalesce(1).write.parquet('s3://project-mahesh/raw_zone/nse10')
df20.coalesce(1).write.saveAsTable('nse20')
