# Spark-Submit--in-dev-environment
This is a small prototype that gives a clear understanding how to do spark-submit in dev environment.

# ![image](https://user-images.githubusercontent.com/70854976/149640258-22699793-7409-44be-8eb4-f79c1be9e5bf.png)

# ![image](https://user-images.githubusercontent.com/70854976/149640287-ab87f5d2-a4d8-4a8f-8f83-ec6300a50a03.png)

# ![image](https://user-images.githubusercontent.com/70854976/149640342-82dc93a4-27c2-424f-b290-6b136911bc09.png)

# Full Code

     import urllib
     from urllib import request
     from pyspark.sql.functions import *
     from pyspark.sql import SparkSession
     from pyspark import SparkConf
     from pyspark import SparkContext

     sc = SparkContext("local", "testing spark code")
     urldata = urllib.request.urlopen("https://randomuser.me/api/0.8/?results=5").read()
     urlstring =urldata.decode("utf8")
     print(urlstring)
     print(urlstring)
     rdd=sc.parallelize([urlstring])

     from pyspark.sql import SparkSession
     spark = SparkSession.builder.getOrCreate()
     df=spark.read.json(rdd)
     df.show()
     df.printSchema()

     flattendf = df.withColumn("results",
     explode(col("results"))).select("nationality","results.user.cell","results.user.dob","results.user.email","results.user.gender","results.user.location.*")
     flattendf.show()

     flattendf.write.format("parquet").mode('overwrite').save("file:///home/cloudera/output_parq")
     




