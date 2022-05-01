import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
string='dsfsds'
import re
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

def printForEachPartition(x):
    rowCount=0
    for i in x:
        print(i)
        rowCount=rowCount+1
    if rowCount!=0:
        print("RowCount in this partition:",rowCount)
def mapFunction(x):
    x=x.split(",")
    placeId=x[0]
    tempType=x[2]
    tempValue=round((int(x[3]) *0.1*9/5) + 32,2)
    return (placeId,(tempType,tempValue))


line= sc.textFile(r"file:///E:/Devesh/Projects/SparkProject/3-Min-MaxTemp/data.csv")
maxTempRDD=line.filter(lambda line:line.find("TMAX") != -1)
maxTempRDD=maxTempRDD.map(mapFunction).partitionBy(4,lambda K:int(re.sub('[^0-9]+', '0', K))%4).reduceByKey(lambda v1,v2:v1 if v1[1]>v2[1] else v2)
maxTempRDD.foreachPartition(printForEachPartition)

minTempRDD=line.filter(lambda line:line.find("TMIN") != -1)
minTempRDD=minTempRDD.map(mapFunction).partitionBy(4,lambda K:int(re.sub('[^0-9]+', '0', K))%4).reduceByKey(lambda v1,v2:v2 if v1[1]>v2[1] else v1)
minTempRDD.foreachPartition(printForEachPartition)
exit(0)