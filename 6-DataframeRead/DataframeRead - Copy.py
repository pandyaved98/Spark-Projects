import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession,Row

spark=SparkSession.builder.appName("DataframeReadExample").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("ERROR")

class Custom:
    def __init__(self,line):
        self.line=line
    def __str__(self):
        return(self.line)

#It will convert csv to Row format
def csvRead(line):
    rowValues=line.split(",")
    return Row(ID=int(rowValues[0]), name=str(rowValues[1].encode("utf-8")),  age=int(rowValues[2]) ,numFriends=int(rowValues[3]))

def csvRead(line):
    return Custom(line)


row_rdd= sc.textFile(r"file:///E:/Devesh/Projects/SparkProject/6-DataframeRead/fakefriends.csv").map(csvRead)

row_rdd.foreach(lambda row:print(row)) #to see O/P in row format
df=spark.createDataFrame(row_rdd)
df.show(1)

#This csv does not contain schema because of that we have to read it in RDD then convert it into DF
