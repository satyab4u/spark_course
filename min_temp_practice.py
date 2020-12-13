from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLines(line):
	fields = line.split(',')
	stationId = fields[0]
	entryType = fields[2]
	temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
	return (stationId, entryType, temperature)


lines = sc.textFile("file:///Users/Satya/Documents/SparkCourse/1800.csv").map(parseLines)

filtered_lines = lines.filter(lambda x: 'TMIN' in x)

tmin_lines = filtered_lines.map(lambda x : (x[0], x[2]))

minTemps = tmin_lines.reduceByKey(lambda x, y : min(x, y))

results = minTemps.collect()

for result in results:
	print(result[0]+"\t{:.2f}F".format(result[1]))





