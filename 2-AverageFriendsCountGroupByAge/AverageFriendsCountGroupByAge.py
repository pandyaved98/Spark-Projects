import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
                        ##implementing map reduce in spark###
############################################################################################################
#  Aim: To find the average of number of friends group by the age(per age group) 
# input formate(id,name,age,totalfriend)
# output: (avg(totalFriend),age_group)
############################################################################################################
#step 1: convert the inp in suitable format (173,Leeta,23,129) to (23,129,1) using map function
#step 3: Partition the rdd 
#step 2: implement combiner 
#       step 3.1: Implement  createCombiner func-> This function access only value part we can modify if something is needed to be changed(step 2 can be implemented)
#       step 3.2: Implement mergeValue-> This function is used to merge the <key,value> pair based on key value in EACH PARTITION of RDD here we have to define how merge value
#       step 3.3: Implement mergeCombiner-> This function is used to merge the <key,value> pair based on key value of all the partion(mergeValue get executed in each partion)
#       (suffle will occur)
#step 3: reduce function for reduce phase

def parseLine(inp):
    line=inp[0]
    lineNumber=inp[1]
    cols=str(line).split(",")
    return (int(cols[2]),(int(cols[3]),1)) #(age,number of friends,1)

def reduce(pair):
    sums=pair[1][0]
    counts=pair[1][1]
    return (pair[0],sums/counts)

def createCombiner(value): 
    return value
def mergeValue(value1,value2):      #value1 is from first <key,value> pair and value2 is from second <key,value> pair
    return (value1[0]+value2[0],value1[1]+value2[1])
def mergeCombiner(value1,value2):   #value1 is from first <key,value> pair and value2 is from second <key,value> pair
    return (value1[0]+value2[0],value1[1]+value2[1])

def reduce(value1,value2):   #value1 is from first <key,value> pair and value2 is from second <key,value> pair
    return (value1[0]+value2[0],value1[1]+value2[1])

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")
partionCount = sc.accumulator(0)


def printForEachPartition(x):
    global partionCount
    rowCount=0
    for i in x:
        print(i)
        rowCount=rowCount+1
    print("RowCount in this partition:",rowCount)
    partionCount.add(1)


lines = sc.textFile("file:///E:/Devesh/Projects/SparkProject/2-AverageFriendsCountGroupByAge/fakefriends.csv")
lines=lines.zipWithIndex()     ##This function will return(line Str,linenumber)
selectCol=lines.map(parseLine) ##This function will select age,friend COL Inp:(id,name,age,totalfriend) to Out:(age,number of friends,1)
selectCol=selectCol.partitionBy(3,lambda key:key%3) ##Partition function
selectCol=selectCol.combineByKey(createCombiner=createCombiner,mergeValue=mergeValue,mergeCombiners=mergeCombiner)
selectCol=selectCol.reduceByKey(reduce)
selectCol=selectCol.mapValues(lambda value:round(value[0]/value[1],2))
selectCol.foreachPartition(printForEachPartition) ##Executes printForEachPartition for each partition
print("Total Partitons:",partionCount.value)