##Problem: Given two nodes find the minimum distance between two nodes in a graph.
import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
import sys
startNode=None
endNode=None
accumEndNodeFound=None #this is a flag which will tell us end is found 0->not found and >0 ->found
#status
UNVISITED=0 #node not found
VISITED=1 #node found But neighbours are not traversed 
EXPLORED=2 #node found and neighbours are traversed

def parseGraph(line):
    arr=line.split(",")
    fromNode=int(arr[0])
    toNodeList=[]
    i=0
    distance=sys.maxsize
    status=UNVISITED
    if fromNode==startNode:
        status=VISITED
        distance=0
    for node in arr[1:]:
        toNodeList.append(int(node))
        i=i+1
    return (fromNode,(toNodeList,distance,status))

def exploreNeighbour(row):
    fromNode=row[0]
    toNode=row[1][0]
    distance=row[1][1]
    status=row[1][2]
    visitedNodeList=[]
    if status==VISITED:
        status=EXPLORED
        for node in toNode:    
            if endNode==node:
                accumEndNodeFound.add(1)  ##endNode found      
            visitedNodeList.append((node,([],distance+1,VISITED)))
        
    visitedNodeList.append((fromNode,(toNode,distance,status)))##to retain the explored node
    return visitedNodeList
#This will merge new discovered node with it's orginal value. 
def merge(node1,node2):
    toNodeList1=node1[0]
    distance1=node1[1]
    status1=node1[2]
    ###
    toNodeList2=node2[0]
    distance2=node2[1]
    status2=node2[2]
    ##to retain all the neighbours 
    toNodeList=toNodeList1+toNodeList2
    ###to retain minimum distance
    distance=distance1 if distance1<distance2 else distance2
    ###to reatin the status status
    status=status1 if status1>status2 else status2    
    return(toNodeList,distance,status)


if __name__ == "__main__":
    conf = SparkConf().setMaster("local[5]").setAppName("Word Count")
    sc = SparkContext(conf = conf)
    sc.setLogLevel("ERROR")
    accumEndNodeFound=sc.accumulator(0)
    startNode=2
    endNode=3
    rd=sc.textFile("file:///E:/Devesh/Projects/SparkProject/9-BFS/inp1.txt").map(parseGraph)
    totalNode=rd.count()
    for i in range(totalNode+1):
        rd=rd.flatMap(exploreNeighbour).reduceByKey(merge)
        rd.count() ###This included because flatMap is lazy accumEndNodeFound will not get updated
        if(accumEndNodeFound.value>0):
            print("EndNode",endNode," found!")
            break
    result=rd.filter(lambda row:row[0]==endNode).collect()
    print("minimum distance between",startNode,'&',endNode,':',result[0][1][1])