package ModelPrediction

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
//Imports needed for Spark
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml._

object Modelpred {
  
  def main(args: Array[String]){
    
    val spark = SparkSession.builder().appName("Bicycle Model Predictor").getOrCreate()
    
    val testdatapath = args(0)
    val modelpath = args(1)
    val outputpath = args(2)
    
    val input = spark.read.format("csv").option("header","true").option("inferschema","true").load(testdatapath)
    
    val testdata = input.withColumn("holiday",input("holiday").cast(StringType)).
                         withColumn("workingday", input("workingday").cast(StringType)).
                         withColumn("season", input("season").cast(StringType)).
                         withColumn("weather", input("weather").cast(StringType))
                        
                            
    val season_testDF = testdata.withColumn("season_1", when(testdata("season")===1,1).otherwise(0)).
                                    withColumn("season_2", when(testdata("season")===2,1).otherwise(0)).
                                    withColumn("season_3", when(testdata("season")===3,1).otherwise(0)).
                                    withColumn("season_4", when(testdata("season")===4,1).otherwise(0)).
                                    drop("season")
  
    val weather_testDF = season_testDF.withColumn("weather_1", when(season_testDF("weather")===1,1).otherwise(0)).
                                          withColumn("weather_2", when(season_testDF("weather")===2,1).otherwise(0)).
                                          withColumn("weather_3", when(season_testDF("weather")===3,1).otherwise(0)).
                                          withColumn("weather_4", when(season_testDF("weather")===4,1).otherwise(0)).
                                          drop("weather")
                                                                           
                         
//    val indexer = Array("season","weather").map(c => new StringIndexer().setInputCol(c).setOutputCol(c + "_Index"))
  
//    val encoder = Array("season","weather").map(column => new OneHotEncoder().setInputCol(column + "_Index").setOutputCol(column + "_Val"))
    
//    val encodedPipeline = new Pipeline().setStages(indexer ++ encoder)
//    val encoded = encodedPipeline.fit(testdata).transform(testdata)
    
//    val indexer1 = { new StringIndexer().setInputCol("holiday").setOutputCol("holidayIndex")}
//
//    val indexer2 = { new StringIndexer().setInputCol("workingday").setOutputCol("workingdayIndex")}
  
//    var testdf = encoded.withColumn("day",split(col("datetime"),"-").getItem(0).cast("int")).withColumn("month",split(col("datetime"), "-").getItem(1).cast("int")).withColumn("year",split(split(col("datetime"),"-")(2)," ")(0).cast("int")).withColumn("hour",split(split(col("datetime")," ")(1),":")(0).cast("int")).drop("weather","season")
    
//    val feature_cols = Array("holiday","workingday","temp","atemp","humidity","windspeed","season_Index","weather_Index","season_Val","weather_Val","hour","day","month","year")

    var datadf = weather_testDF.withColumn("day",split(col("datetime"),"-").getItem(0).cast("int")).withColumn("month",split(col("datetime"),"-").getItem(1).cast("int")).withColumn("year",split(split(col("datetime"),"-")(2)," ")(0).cast("int")).withColumn("hour",split(split(col("datetime")," ")(1),":")(0).cast("int"))
  
//    val feature_cols = Array("holiday","workingday","temp","atemp","humidity","windspeed","season_Index","weather_Index","season_Val","weather_Val","hour","day","month","year")
//    val feature_cols = Array("holiday","workingday","temp","atemp","humidity","windspeed","casual","registered","season_Index","weather_Index","season_Val","weather_Val","hour","day","month","year")
    
//    val assembler = new VectorAssembler().setInputCols(feature_cols).setOutputCol("features")
  
   
//    val testoutput = assembler.transform(datadf).select("features","datetime")
    
    val model = PipelineModel.read.load(modelpath)
//    val pipeline = new Pipeline().setStages(Array(indexer1, indexer2, assembler, model))
    
    
    val predictions = model.transform(datadf)
//    val predictions = model.transform(testoutput).withColumn("datetime",concat(col("year"),lit("-"),col("month"),lit("-"),col("day"),lit(" "),col("hour"),lit(":00:00")))
//    val predictions = pipeline.fit(datadf).transform(datadf)
    
    println("***********************Printing prediction Schema**************************")
    predictions.printSchema
    
//    println("Sample Predicted record: "+predictions.show(1))
//    println("Predicted record: "+predictions.select("features","prediction").show(1))    
    
    val predfile = predictions.select("prediction","datetime")
    
    println("**********************Showing sample predicted data**************************")
    predfile.show(10)
    //Writing to HDFS location
    predfile.coalesce(1).write.format("csv").mode("overwrite").save(outputpath)
    
  }
  
}