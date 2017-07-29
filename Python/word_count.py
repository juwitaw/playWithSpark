from pyspark.sql import SparkSession

spark = SparkSession.builder \
	.master("local[*]") \
	.appName("word_count") \
	.getOrCreate()
sc = spark.sparkContext

text = ["apache spark is a fast and general-purpose cluster computing system", 
"it provides high-level APIs in java scala python and R and an optimized engine that \
supports general execution graphs", "it also supports a rich set of higher-level tools including \
spark SQL for SQL and structured data processing MLlib for machine learning GraphX for graph \
processing and spark streaming"]

#rdd-based
text_rdd = sc.parallelize(text)
counts_rdd = text_rdd.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
result_rdd = counts_rdd.collect()
print(result_rdd)

#dataframe-based
from pyspark.sql.functions import split, explode, lit

text_df = spark.createDataFrame([(sentence,) for sentence in text], ['sentence'])
text_df = text_df.withColumn("list_of_words", split(text_df['sentence'], " "))
words_df = text_df.select(explode(text_df['list_of_words']))
result_df = words_df.groupBy("col").count()
result_df.show(100)