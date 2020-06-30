package ModelStreaming

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
//Imports needed for Spark
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext._;
import org.apache.spark.streaming.StreamingContext;
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext;
import org.apache.spark.ml.feature.{ OneHotEncoder, StringIndexer }
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.Encoders
import org.apache.spark.ml._

object ModelStream {

  case class Data(datetime: String, season: Int, holiday: Int, workingday: Int, weather: Int,
    temp: Double, atemp: Double, humidity: Int, windspeed: Double)

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Streaming Model")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val lines = ssc.textFileStream("/user/edureka_893377/Input/Spark/Final/FlumeHDFS")

    lines.foreachRDD {
      rdd =>
        {
          if (!rdd.isEmpty) {
            val input = rdd.map(x => x.split(",")).
              map(data => Data(data(0).toString, data(1).toInt, data(2).toInt, data(3).toInt, data(4).toInt,
                data(5).toDouble, data(6).toDouble, data(7).toInt, data(8).toDouble)).toDF()

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
                         
   
            var testdf = weather_trainDF.withColumn("day", split(col("datetime"), "-").getItem(0).cast("int")).withColumn("month", split(col("datetime"), "-").getItem(1).cast("int")).withColumn("year", split(split(col("datetime"), "-")(2), " ")(0).cast("int")).withColumn("hour", split(split(col("datetime"), " ")(1), ":")(0).cast("int"))

            val model = PipelineModel.read.load("/user/edureka_893377/Input/Spark/Final/Model")
            val predictions = model.transform(testdf).select("datetime","prediction")
            
            println("Sample predictions from streaming data")
            predictions.show(2)
            
            // saving to DB              
             predictions.write.format("jdbc").mode("overwrite").option("url", "jdbc:mysql://mysqldb.edu.cloudlab.com/edureka_893377").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "predictions").
             option("user", "labuser").option("password", "edureka").save()
              
          } else {
            print("********************************************************")
            print("RDD is empty")
          }
        }
    }

    ssc.start

    ssc.awaitTermination

  }

}
