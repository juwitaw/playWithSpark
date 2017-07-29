from pyspark.sql import SparkSession

spark = SparkSession.builder \
	.master("local[*]") \
	.appName("word_count") \
	.getOrCreate()
sc = spark.sparkContext

#rdd-based
text_rdd = sc.parallelize(["Apache Spark is a fast and general-purpose cluster computing system", 
"It provides high-level APIs in Java, Scala, Python and R, and an optimized engine that \
supports general execution graphs", "It also supports a rich set of higher-level tools including \
Spark SQL for SQL and structured data processing, MLlib for machine learning, GraphX for graph \
processing, and Spark Streaming"])
counts = text_rdd.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
result = counts.collect()
print(result)

#dataframe-based
text_df = text_rdd.toDF()
text_df.show()