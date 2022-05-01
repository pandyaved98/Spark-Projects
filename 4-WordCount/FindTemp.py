import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
import re
conf = SparkConf().setMaster("local[5]").setAppName("Word Count")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

def printForEachPartition(x):
    rowCount=0
    for i in x:
        print(i)
        rowCount=rowCount+1
    if rowCount!=0: 
        print("RowCount in this partition:",rowCount)
def mapFunction(line):
    line=re.sub('[^a-zA-Z]'," ",line).upper().split()
    return line

#flatMap takes array as input and split it in the ouptut 
# [1,2,3]->flatMap->1 2 3
#[[1,2,3],4]->flatMap->[1,2,3] 4
line= sc.textFile(r"file:///E:/Devesh/Projects/SparkProject/4-WordCount/book.txt")

coll=line.flatMap(mapFunction).map(lambda x:(x,1)).lookup('THE')
print(coll)
exit(0)
line=line.flatMap(mapFunction).map(lambda x:(x,1)).partitionBy(26,lambda key:(ord(key[0])-65)%26)\
.combineByKey(lambda V:V,lambda V1,V2:V1+V2,lambda V1,V2:V1+V2)\
.reduceByKey(lambda V1,V2:V1+V2)\
.sortBy(lambda x:x[1]).collect()



for l in line:
    print(l)
#line.foreachPartition(printForEachPartition)
