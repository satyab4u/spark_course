from pyspark import SparkConf, SparkContext

'''
def loadMovieNames():
	movieNames = {}
	with open("/Users/Satya/Documents/SparkCourse/ml-100k/u.item",'rb') as f:
		for line in f:
			fields = line.split('|')
			movieNames[int(fields[0])] = str(fields[1])
	return movieNames
'''
def loadMovieNames():
    movieNames = {}
    with open("/Users/Satya/Documents/SparkCourse/ml-100k/u.item", encoding = 'UTF-8', errors ='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames			

conf = SparkConf().setMaster("local").setAppName("popular_movie_app")
sc = SparkContext(conf = conf)

namedDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("/Users/Satya/Documents/SparkCourse/ml-100k/u.data")
movie_rdd = lines.map(lambda x: (int(x.split()[1]), 1))

movie_count = movie_rdd.reduceByKey(lambda x, y : x + y)

flipped = movie_count.map(lambda x: (x[1], x[0]))

sorted_flipped = flipped.sortByKey(False)
sorted_movie_names = sorted_flipped.map(lambda x: (namedDict.value[x[1]], x[0]))
#.collect()


#movie_result = movie_count.collect()
results = sorted_movie_names.collect()

for result in results:
	print(result[0], result[1])

print('highest watched movie',results[0][0], results[0][1])	


