from pyspark import SparkContext, SparkConf
conf=SparkConf().setAppName('wordcount')
sc=SparkContext(conf=conf)
data=sc.textFile('file:///home/an09mous/Desktop/test.txt')
lines=data.flatMap(lambda x: x.split())
pairs=lines.map(lambda x: (x,1))
counts=pairs.reduceByKey(lambda a,b: a+b)
counts.saveAsTextFile('file:///home/an09mous/Desktop/output')
