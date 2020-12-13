from pyspark import SparkConf, SparkContext

'''
def parseLines(line):
    data = line.split(',')
	custId = int(data[0])
	amount = int(data[2])
	return (custId, amount)
'''

def parseLine(line):
    fields = line.split(',')
    cust = int(fields[0])
    amt = float(fields[2])
    return (cust, amt)


conf = SparkConf().setMaster("local").setAppName("AvgSpendingApp")

sc = SparkContext(conf = conf)

cust_orders = sc.textFile("/Users/Satya/Documents/SparkCourse/customer-orders.csv").map(parseLine)

total_amount_orders = cust_orders.reduceByKey(lambda x, y : x + y)

cust_amounts = total_amount_orders.collect()


for (custId, amount) in cust_amounts:
	print(custId, round(amount,2))



