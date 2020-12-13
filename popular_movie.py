from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("popular_movie_app")
sc = SparkContext(conf = conf)
def parseLine(line):
	fields = line.split()
	return (fields[1], 1)

lines = sc.textFile("/Users/Satya/Documents/SparkCourse/ml-100k/u.data")
movie_rdd = lines.map(parseLine)

movie_count = movie_rdd.reduceByKey(lambda x, y : x + y)

flipped = movie_count.map(lambda x: (x[1], x[0]))

sorted_flipped = flipped.sortByKey(False).collect()


#movie_result = movie_count.collect()

for result in sorted_flipped:
	print(result[0], result[1])

print('highest watched movie',sorted_flipped[0][1], sorted_flipped[0][0])	


