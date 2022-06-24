from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
#sc = SparkContext(conf = conf)
sc = SparkContext.getOrCreate()

def parseLine(line):
    fields = line.split(',')
    age = round(int(fields[2]),0)
    numFriends = round(int(fields[3]),0)
    return (age, numFriends)

lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
