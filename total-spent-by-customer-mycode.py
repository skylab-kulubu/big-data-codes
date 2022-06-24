from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SumOfCustomerSpent")
#sc = SparkContext(conf = conf)
sc = SparkContext.getOrCreate()

input = sc.textFile("file:///sparkcourse/customer-orders.csv")

def parseLine(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

mappedInput = input.map(parseLine)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)
results = totalByCustomer.collect()
for result in results:
    print(result)