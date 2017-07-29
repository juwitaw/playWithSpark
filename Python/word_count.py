from pyspark.sql import SparkSession

spark = SparkSession.builder \
	.master("local[*]") \
	.appName("word_count") \
	.getOrCreate()
sc - spark.sparkContext

# text_file = sc.textFile("hdfs://...")
# counts = text_file.flatMap(lambda line: line.split(" ")) \
#              .map(lambda word: (word, 1)) \
#              .reduceByKey(lambda a, b: a + b)
# counts.saveAsTextFile("hdfs://...")