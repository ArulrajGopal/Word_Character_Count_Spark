!pip install pyspark

from pyspark.sql import SparkSession

#create spark session
spark= SparkSession.builder.appName('mysparksession').getOrCreate()

#create spark context
sc = spark.sparkContext


text_file = sc.textFile('/content/drive/MyDrive/python_home_folder/passage.txt')

text_file = text_file.flatMap(lambda x: [*x])\
.map(lambda x:(x,1))\
.reduceByKey(lambda x,y: x+y)

text_file.collect()
