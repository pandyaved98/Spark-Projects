
#CustomSchema while reading any file types
from tkinter import S
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,IntegerType,StringType,FloatType,StructField


spark=SparkSession.builder.appName("DataframeReadExample").getOrCreate()

customSchema=StructType([
    StructField("stationId",StringType(),True) ,\
    StructField("date",IntegerType(),True) ,\
    StructField("measure_type",StringType(),True),\
    StructField("temperature",FloatType(),True)\
])

#df=spark.read.schema(customSchema).\
#csv(r"file:///E:/Devesh/Projects/SparkProject/7-CustomSchema/temp.csv").show()
spark.createDataFrame([1])