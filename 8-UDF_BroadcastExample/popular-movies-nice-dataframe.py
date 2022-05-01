from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs
#Function will convert raw data into dict. This dict will be passed as broadcast variable
def loadMovieNames():
    movieNames = {}
    with codecs.open("E:/Devesh/Projects/SparkProject/8-UDF_BroadcastExample/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames
spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames()) #Broadcasting var
# Create schema when reading u.data
schema = StructType([StructField("userID", IntegerType(), True), \
StructField("movieID", IntegerType(), True), \
StructField("rating", IntegerType(), True), \
StructField("timestamp", LongType(), True)])
# Load up movie data as dataframe. u.data doesnot contain headers
moviesDF = spark.read.option("sep", "/t").schema(schema).csv("file:///E:/Devesh/Projects/SparkProject/8-UDF_BroadcastExample/u.data") 
movieCounts = moviesDF.groupBy("movieID").count()
# Create a user-defined function to look up movie names from broadcasted dictionary
def lookupName(movieID):
    return nameDict.value[movieID]

#This will register the function which will help in lookup because we cannot directly use Broadcast var with DF
lookupNameUDF = func.udf(lookupName)
# Add a movieTitle column using our new udf
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID"))) ##Func
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))
sortedMoviesWithNames.show(10, False)
spark.stop()
