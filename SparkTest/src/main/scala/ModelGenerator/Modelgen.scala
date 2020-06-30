package ModelGenerator

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
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator



object Modelgen {
  def main(args: Array[String]){
    
    if (args.length < 1){
      System.err.println("Usage: Model generator <datapath> <model persistance path>")
      System.exit(1)
    }
    

  //SparkContext
  // val sparkConf = new SparkConf().setAppName("Spark Model")
  // val sc = new SparkContext(sparkConf)
  // val file = sc.textFile(logFile)
  
  
  //Spark
  val spark = SparkSession.builder().appName("Bicycle Model Generator").getOrCreate()
  
  val datapath = args(0)
  val modelpath = args(1)
  
//  val traindata = spark.read.format("csv").option("header","true").option("inferschema","true").load("/user/edureka_893377/Input/Spark/Final/train.csv")
  val input = spark.read.format("csv").option("header","true").option("inferschema","true").load(datapath)

  val traindata = input.withColumn("holiday",input("holiday").cast(StringType)).
                        withColumn("workingday", input("workingday").cast(StringType)).
                        withColumn("season", input("season").cast(StringType)).
                        withColumn("weather", input("weather").cast(StringType))
                        
  val season_trainDF = traindata.withColumn("season_1", when(traindata("season")===1,1).otherwise(0)).
                                  withColumn("season_2", when(traindata("season")===2,1).otherwise(0)).
                                  withColumn("season_3", when(traindata("season")===3,1).otherwise(0)).
                                  withColumn("season_4", when(traindata("season")===4,1).otherwise(0)).
                                  drop("season")

  val weather_trainDF = season_trainDF.withColumn("weather_1", when(season_trainDF("weather")===1,1).otherwise(0)).
                                        withColumn("weather_2", when(season_trainDF("weather")===2,1).otherwise(0)).
                                        withColumn("weather_3", when(season_trainDF("weather")===3,1).otherwise(0)).
                                        withColumn("weather_4", when(season_trainDF("weather")===4,1).otherwise(0)).
                                        drop("weather")
                                
                                  
//Not using String Indexer and encoder since unable to convert during predicting  
  /*val indexer = Array("holiday","workingday","season","weather").map(c => new StringIndexer().setInputCol(c).setOutputCol(c + "_Index"))
    
  val encoder = Array("season","weather").map(column => new OneHotEncoder().setInputCol(column + "_Index").setOutputCol(column + "_Val"))
  */
                                        
//  val encodedPipeline = new Pipeline().setStages(indexer ++ encoder)
//  val encoded = encodedPipeline.fit(traindata).transform(traindata)
  
//  var datadf = encoded.withColumn("day",split(col("datetime"),"-").getItem(0).cast("int")).withColumn("month",split(col("datetime"),"-").getItem(1).cast("int")).withColumn("year",split(split(col("datetime"),"-")(2)," ")(0).cast("int")).withColumn("hour",split(split(col("datetime")," ")(1),":")(0).cast("int")).withColumnRenamed("Count","label").drop("weather","season")
  var datadf = weather_trainDF.withColumn("day",split(col("datetime"),"-").getItem(0).cast("int")).withColumn("month",split(col("datetime"),"-").getItem(1).cast("int")).withColumn("year",split(split(col("datetime"),"-")(2)," ")(0).cast("int")).withColumn("hour",split(split(col("datetime")," ")(1),":")(0).cast("int")).withColumnRenamed("Count","label")
  
  val splitSeed = 123
  val Array(train,test) = datadf.randomSplit(Array(0.7,0.3), splitSeed)
  
  val indexer1 = { new StringIndexer().setInputCol("holiday").setOutputCol("holiday_Index")}

  val indexer2 = { new StringIndexer().setInputCol("workingday").setOutputCol("workingday_Index")}
  
  val feature_cols = Array("holiday_Index","workingday_Index","temp","atemp","humidity","windspeed","season_1","season_2","season_3","season_4","weather_1","weather_2","weather_3","weather_4","hour","day","month","year")

//  val feature_cols = Array("holiday","workingday","temp","atemp","humidity","windspeed","season_Index","weather_Index","season_Val","weather_Val","hour","day","month","year")
  val assembler = new VectorAssembler().setInputCols(feature_cols).setOutputCol("features")

//  val output = assembler.transform(datadf).select("label","features")
  
//  val lr = new LinearRegression()
//   val model = lr.fit(output)
  val gbt = new GBTRegressor().setLabelCol("label").setFeaturesCol("features").setMaxIter(10)
  val pipeline = new Pipeline().setStages(Array(indexer1,indexer2,assembler, gbt))
//  val pipeline = new Pipeline().setStages(Array(assembler, gbt))
  
  val model = pipeline.fit(train)
  
  val predictions = model.transform(test)
  val evaluator = { new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")}
  
  val rmse = evaluator.evaluate(predictions)
  
  println("**************************************************************************")
  
  println("Root mean square error: " +rmse)
  
  println("sample model record: "+predictions.show(5))
  
  predictions.printSchema()
  
  //Save model
  println("Saving Model")
  model.write.overwrite().save(modelpath)
//  spark.stop() 
  
  }

}