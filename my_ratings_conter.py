from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("MyRatingHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///Users/Satya/Documents/SparkCourse/ml-100k/u.data")

#print('lines', lines)

ratings = lines.map(lambda x : x.split()[2])

#print('ratings', ratings)

result = ratings.countByValue()

#print('result', result)

sortedResult = collections.OrderedDict(sorted(result.items()))

for key, value in sortedResult.items():
	print(key, value)
