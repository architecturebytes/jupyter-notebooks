!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
!tar xf spark-3.1.1-bin-hadoop3.2.tgz
!pip install -q findspark

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.1-bin-hadoop3.2"

import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
spark

csvDF = spark.read.csv("data/retail.csv", header=True, sep=",", inferSchema=True)

csvDF.show()

csvDF.printSchema()

from pyspark.sql.functions import to_date 
csvDF1 = csvDF.withColumn("order_date",to_date('order_date', 'dd-mm-yyyy')).fillna({'order_date': '2000-01-01'})

csvDF1.printSchema()

csvDF1.count()

csvDF1.show()

csvDF2 = csvDF1.dropDuplicates()

csvDF2.count()

csvDF2.show()

csvDF2.select("order_no","order_date","amount")

csvDF2.select("order_no","order_date","amount").where(csvDF2.amount>100)

csvDF2.groupBy("cust_country").agg({'amount':'sum'}).show()

csvDF2.createOrReplaceTempView('orders')

spark.sql("select * from orders").show()

spark.sql("select cust_country, sum(amount) from orders group by cust_country").show()

csvDF2.write.csv("data/out")

csvDF3 = csvDF2.repartition("cust_country")

csvDF3.write.option("header",False).partitionBy("cust_country").csv("data/out1")
