import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
	return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("Word_count_app")
sc = SparkContext(conf = conf)

lines = sc.textFile("/Users/Satya/Documents/SparkCourse/book.txt")

words = lines.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

wordsCountsSorted = wordCounts.map(lambda x : (x[1], x[0])).sortByKey()

results = wordsCountsSorted.collect()

for result in results:
	count= str(result[0])
	word = result[1].encode('ascii','ignore')
	if (word):
		print(word, count)

#wordCounts = words.countByValue()
'''
for word, count in wordCounts.items():
	cleanWord = word.encode('ascii','ignore')
	if(cleanWord):
		print(cleanWord, count)
'''

