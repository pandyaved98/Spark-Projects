#Example to explain read CSV without headers and to convert RDD to DF
import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession,Row

#It will convert csv to Row format
def csvRead(line):
    rowValues=line.split(",")
    return Row(ID=int(rowValues[0]), name=str(rowValues[1].encode("utf-8")),  age=int(rowValues[2]) ,numFriends=int(rowValues[3]))


spark=SparkSession.builder.appName("DataframeReadExample").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("ERROR")

#This csv does not contain schema because of that we have to read it in RDD then convert it into DF
row_rdd= sc.textFile(r"file:///E:/Devesh/Projects/SparkProject/6-DataframeRead/fakefriends.csv").map(csvRead)

#row_rdd.foreach(lambda row:print(row)) #to see O/P in row format
df=spark.createDataFrame(row_rdd)
df.createOrReplaceTempView("friends")
transformed_df=spark.sql("select * from friends where age>=13")

#transformed_df.foreach(lambda x:print(x[0],x[1],x[2],x[3]))  OR  
transformed_df.foreach(lambda x:print(x['ID'],x['name'],x['age'],x['numFriends']))



