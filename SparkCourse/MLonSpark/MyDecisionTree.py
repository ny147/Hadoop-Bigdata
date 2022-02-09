from __future__ import print_function

from pyspark.ml.regression import LinearRegression

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

if __name__ == "__main__":

    # Create a SparkSession (Note, the config section is only for Windows!)
    spark = SparkSession.builder.appName("DecisionTree").getOrCreate()
    schema = StructType([\
                     StructField("No", IntegerType(), True), \
                     StructField("TransactionDate",FloatType(), True), \
                     StructField("HouseAge", FloatType(), True), \
                     StructField("DistanceToMRT", FloatType(), True),\
                     StructField("NumberConvenienceStores", IntegerType(), True),\
                     StructField("Latitude", FloatType(), True),\
                     StructField("Longitude", FloatType(), True),\
                     StructField("PriceOfUnitArea", FloatType(), True)\

    ])

# // Read the file as dataframe
    fileData = spark.read.schema(schema).csv("C:/Users/Near/Desktop/SparkCourse/csv/realestate.csv")
    data = fileData.select("No","TransactionDate","HouseAge",\
                                                "DistanceToMRT","NumberConvenienceStores","Latitude","Longitude")
    LabelsWithNo = fileData.select("No","PriceOfUnitArea")
    # assembler = VectorAssembler().setInputCols(["No","TransactionDate","HouseAge",\
    #                                             "DistanceToMRT","NumberConvenienceStores","Latitude","Longitude"])
    assembler = VectorAssembler(inputCols=["No","TransactionDate","HouseAge",\
                                                "DistanceToMRT","NumberConvenienceStores","Latitude","Longitude"]\
                                                    , outputCol="features", handleInvalid="skip")
    df = assembler.transform(data).select("No","features")
    
    dataset = df.join(LabelsWithNo,"No").drop("No")
    dataset = dataset.withColumnRenamed("PriceOfUnitArea","label")
    dataset = dataset.select("label","features")
 
 

    # Let's split our data into training data and testing data
    trainTest = dataset.randomSplit([0.5, 0.5])
    trainingDF = trainTest[0]
    testDF = trainTest[1]
    

    Dicision = DecisionTreeRegressor(maxDepth=5)
    model = Dicision.fit(trainingDF)
    
    result = model.transform(testDF).cache()
    prediction = result.select("prediction","label").show()
    


    # # Stop the session
    spark.stop()
