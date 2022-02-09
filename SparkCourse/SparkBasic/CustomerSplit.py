from pyspark import SparkConf, SparkContext

def splitdata(line):
    
    fields = line.split(',')
    ID = fields[0]
    Item= fields[1]
    price = float(fields[2])
    return (ID , price )

conf = SparkConf().setMaster("local").setAppName("Customer")
sc = SparkContext(conf = conf)

input = sc.textFile("C:/Users/Near/Desktop/SparkCourse/customer-orders.csv")
order = input.map(splitdata)
totalorder = order.reduceByKey(lambda x,y : x+y)
sortOrder = totalorder.map(lambda x: (x[1], x[0])).sortByKey()
results = sortOrder.collect()
for result in results:
    print(str(result[1]) + " : "  +  str(result[0]))
    