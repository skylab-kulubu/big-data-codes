from __future__ import print_function

from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

if __name__ == "__main__":

    # Create a SparkSession (Note, the config section is only for Windows!)
    spark = SparkSession.builder.appName("DecisionTree").getOrCreate()

    data = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///SparkCourse/realestate.csv")

    assembler = VectorAssembler().setInputCols(["HouseAge", "DistanceToMRT","NumberConvenienceStores"]).setOutputCol("features")
    
    df = assembler.transform(data).select("PriceOfUnitArea", "features")
    
    trainTest = df.randomSplit([0.94, 0.06])
    trainingDF = trainTest[0]
    testDF = trainTest[1]
    
    dtr = DecisionTreeRegressor().setFeaturesCol("features").setLabelCol("PriceOfUnitArea")
    
    
    model = dtr.fit(trainingDF)
    
    fullPredictions = model.transform(testDF).cache()
    
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])
    
    predictionAndLabel = predictions.zip(labels).collect()
    
    for prediction in predictionAndLabel:
      print(prediction)
      
      spark.stop()