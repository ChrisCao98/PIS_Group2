package alg


import Util.{PETUtils, RedisUtil, RedisWriter, SaveInfo_Java}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}

import java.sql.Timestamp
import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * Here is a variation for StructuredStreaming to test, if the pipeline can be reconstructed during the running.
 * It proved that the StructuredStreaming can't reconstructed during the running by simply changing the function, which is
 * already executed in the stream.
 */

object StructuredStreaming01 {
  private val BOOTSTRAP_SERVERS = "localhost:9092"
  val path: String = "config/Pipeconfig.json"
  val pathInfo = PathInfo(path)
  initialize()

  def initialize(): Unit = {
    val client = RedisUtil.getJedisClient
    client.set("SpeedPET", "0")
    client.set("LocationPET", "0")
    client.set("CameraPET", "0")
    client.set("CameraSituation", "0")
    client.set("LocationSituation", "0")
    client.set("SpeedSituation", "0")


    client.close()
  }


  val spark: SparkSession = SparkSession
    .builder()
    .appName("Streaming process from Kafka")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.default.parallelism", "1")
    .getOrCreate()

  import spark.implicits._

  def load(TopicName:String):DataFrame = {
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
      .option("subscribe", TopicName)
      .load()
    df
  }
  /**
   *This is the object,which is used to reconstruct the pipeline.
   *  The function "select" is designed to dynamically change the stream, but this idea can't realize it.
   *  Because in the main function, all the process was executed once before the datum are received.
   *  That means the output of select function can't be changed.
   *  That's why this idea is failed.
   */
  object PipeLineReconstruct {
    private var start_gps = true
    private var start_img = false
    private var start_location = false

    def initialize(): Unit = {
      val client = RedisUtil.getJedisClient
      start_gps = client.get("start_gps").toBoolean
      start_location = client.get("start_location").toBoolean
      start_img = client.get("start_img").toBoolean
      client.close()
    }

    private var df_1: DataFrame = _
    private var df_2: DataFrame = _

    def select(): DataFrame = {
      println("Spark:"+spark)
      println("zhixingla")
      //      sparkSession_all = sparkSession
      initialize()
      println("start_gps: " + start_gps)
      println("start_img: " + start_img)
      if (start_gps) {
        println("gps")
        println("Spark: " + spark)
        df_1 = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
          .option("subscribe", "test-data")
          .load()
      }
      if (start_img) {
        println("img")
        df_1 = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
          .option("subscribe", "test-image")
          .load()
      }
      df_1
    }
  }


  var StartSpeedLocaton = true
  var StartImage = false
  private var a = "false"
  private var query_gps: StreamingQuery =_
  private var query_image: StreamingQuery =_
  private var count: Int = 0


  def main(args: Array[String]): Unit = {

    implicit val saveInfoEncoder: Encoder[SaveInfo_Java] = Encoders.javaSerialization[SaveInfo_Java]
    if (StartSpeedLocaton) {
      println(a.toBoolean)
    }
    //load data
    if(StartSpeedLocaton){
      println("excecute df_gps")
    }
    //    val df_gps = load("test-data")
    //    val df_image = load("test-image")
    val df_input = load("test-user-input")
    val df = PipeLineReconstruct.select()
//    val df_gps = df._1
//    val df_image = df._2
//    if (df_gps != null){
    val Ds_GPS = df
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(new PETUtils().TakeSomeInfo, saveInfoEncoder)
      .map(new PETUtils().Evaluation, saveInfoEncoder)
      .map(new PETUtils().ApplyPET(pathInfo.getPETconfpath, "SPEED"), saveInfoEncoder)
      //      .map(new PETUtils().ApplyPET(pathInfo.getPETconfpath, "LOCATION"),saveInfoEncoder)
      .map { SaveInfo_Java
      =>
        FinalGPSEvent(new Timestamp(SaveInfo_Java.getTimestamp.toLong),
          SaveInfo_Java.getLocation.asScala.toList,
          SaveInfo_Java.getAltitude,
          SaveInfo_Java.getAcc_x,
          SaveInfo_Java.getAcc_y,
          SaveInfo_Java.getVel
//          SaveInfo_Java.getTimerRecord.asScala.toList
        )
        }
      query_gps = Ds_GPS.toDF()
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", false)
        //      .trigger(Trigger.Continuous("10 milliseconds"))
        .start()
//    }
//    println("1")
//    if (df_image != null) {
//      println("df_image zai ex")
//      val DS_Image = df_image
//        .select("value")
//        .as[Array[Byte]]
//        .map(new PETUtils().AddImageInfo, saveInfoEncoder)
//        .map(new PETUtils().EvaluationImage, saveInfoEncoder)
//        .map(new PETUtils().ApplyPET(pathInfo.getPETconfpath, "IMAGE"), saveInfoEncoder)
//        .map {
//          saveInfo_Java =>
//            saveInfo_Java.getImg
//        }.toDF()
//      query_image = DS_Image
//        .writeStream
//        //      .foreachBatch(new ForeachWriter[Array[Byte]] {})
//        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
//          batchDF.foreach { row =>
//            val imageBytes = row.getAs[Array[Byte]]("value")
//            pathInfo.getGUI_img.displayImage(imageBytes)
//          }
//        }
//        .start()
//    }


    //process user input
    val DS_User = df_input
      .selectExpr("CAST(value AS STRING)")
      .as[String]


    //      }
    println("1")


    val query_user = DS_User
      .writeStream
      .foreach(new RedisWriter)
      .start()

//    if (df_gps != null) {
//      query_gps.awaitTermination()
//
//    }
    query_gps.awaitTermination()
//    if (df_image != null) {
//      println("image?")
//      query_image.awaitTermination()
//
//    }
    //    query_image.awaitTermination()
    query_user.awaitTermination()

  }
}
