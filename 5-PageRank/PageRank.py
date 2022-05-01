import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
import re
conf = SparkConf().setMaster("local[5]").setAppName("Word Count")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

#flatMap takes array as input and split it in the ouptut 
# [1,2,3]->flatMap->1 2 3
#[[1,2,3],4]->flatMap->[1,2,3] 4

#Input file link: https://snap.stanford.edu/data/web-Google.html
def foreachFunc(partition):
    for line in partition:
        for l in line:
            print(l)

def distributeRank(row):
    toNodeList=row[0]
    rank=row[1]
    distributedRankValue=rank/len(toNodeList)
    newNodeRank=list()
    for toNode in toNodeList:
        if int(toNode)!=-1:
            newNodeRank.append([toNode,distributedRankValue])
    #print(newNodeRank)
    return(newNodeRank)
def rankSum(ranks):
    total=0
    for rank in ranks:
        if rank==None:
            continue
        total=rank+total
    return total
file= sc.textFile(r"file:///E:/Devesh/Projects/SparkProject/Spark-Projects/5-PageRank/web-Google_1.txt").filter(lambda line:'#' not in line)\
.map(lambda line:line.split('\t'))
links=file.partitionBy(4,lambda x:int(x[0])%4).groupByKey().cache() #Will return [fromNode,toNodeList[]]
ranks=links.map(lambda line:[line[0],1.0]) #Will return [fromNode,1.0] where 1.0 is the rank
for x in range(2):
    links_rank=links.join(ranks) #Will return [fromNode,[toNodeList[],rank]]
    links_rank=links_rank.flatMap(lambda row:distributeRank(row[1])) #row[1] means [toNodeList[],rank]
    newRank=links_rank.reduceByKey(lambda x,y:x+y) #sum all the ranks of same node
    ranks=ranks.leftOuterJoin(newRank).mapValues(rankSum)
    #print(ranks.collect())
#print(ranks.collect())
ranks.saveAsTextFile("file:///E:/Devesh/Projects/SparkProject/5-PageRank/out.txt")