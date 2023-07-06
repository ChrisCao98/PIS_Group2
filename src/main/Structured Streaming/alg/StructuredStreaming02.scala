package alg

import Util.{PETUtils, RedisUtil, SaveInfo_Java}
import alg.StructuredStreaming.pathInfo
import org.apache.spark.sql.functions.{col, expr, lit, monotonically_increasing_id}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, ForeachWriter, Row, SparkSession}

import java.sql.Timestamp

object StructuredStreaming02 {
  private val BOOTSTRAP_SERVERS = "localhost:9092"
  val path: String = "config/Pipeconfig.json"
  val pathInfo = PathInfo(path)
  initialize()

  def initialize(): Unit = {
    val client = RedisUtil.getJedisClient
    client.set("SPEED", "0")
    client.set("LOCATION", "0")
    client.set("IMAGE", "0")
    client.set("start_gps","true")
    client.set("start_location","false")
    client.set("start_img","false")
    client.close()
  }


  val spark: SparkSession = SparkSession
    .builder()
    .appName("Streaming process from Kafka")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.default.parallelism", "1")
    .getOrCreate()

  def load(TopicName:String):DataFrame = {
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
      .option("subscribe", TopicName)
      .load()
    df
  }

  def main(args: Array[String]): Unit = {

    implicit val saveInfoEncoder: Encoder[SaveInfo_Java] = Encoders.javaSerialization[SaveInfo_Java]
    val df_gps = load("test-data")
    val df_image = load("test-image")
    val df_input = load("test-user-input")
    import spark.implicits._
    val Ds_GPS = df_gps
      .selectExpr("CAST(value AS STRING) AS GPS" ,"CAST(timestamp AS Timestamp)")
      .withColumn("index", lit(1))
      .select($"GPS",$"index",$"timestamp".alias("timestamp1"))
//      .as[(String, Long)]
      .as[(String,Long,Timestamp)]
      .withWatermark("timestamp1", "2 seconds")
    Ds_GPS.printSchema()

    val DS_Image = df_image
      .select(col("value").alias("image"),expr("CAST(timestamp AS Timestamp)")
      )
      .withColumn("index", lit(1))
      .select($"image", $"index".alias("newIndex"),$"timestamp".alias("timestamp2"))
      .as[(Array[Byte],Long,Timestamp)]
      .withWatermark("timestamp2", "2 seconds")

    val joinCondition = col("index") === col("newIndex") && col("timestamp1").between(expr("timestamp2 - interval 2 seconds"),col("timestamp2") )
    val Df = Ds_GPS.join(DS_Image,joinCondition,"left")
//    val joinCondition = col("index") === col("newIndex") && col("timestamp1").between(expr("timestamp2 - interval 1 minute"),col("timestamp2") )
//    val Df = Ds_GPS.join(DS_Image,joinCondition,"inner")
//        .withWatermark("timestamp1", "30 seconds")
      .dropDuplicates("GPS")
//      .dropDuplicates("GPS")
      .select("GPS", "image")
      .as[(String,Array[Byte])]
      .map{
        lines =>
          val words = lines._1.split(",")
          val saveInfo_Java:SaveInfo_Java= new SaveInfo_Java(words(0).toDouble,
            words(1).toDouble, words(2).toDouble,
            words(3).toDouble, words(12).toDouble,
            words(13).toDouble, words(9).toDouble
          )
          val imageB = lines._2
          saveInfo_Java.setImg(imageB)
          println(saveInfo_Java.getImg)
          saveInfo_Java
      }
      .map(new PETUtils().EvaluationImage, saveInfoEncoder)
      .map(new PETUtils().ApplyPET(pathInfo.getPETconfpath, "IMAGE"), saveInfoEncoder)
      .map {
        saveInfo_Java =>
          saveInfo_Java.getImg
      }.toDF()
    Df.printSchema()

    val query_image = Df
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.foreach { row =>
          println("paole?")
          val imageBytes = row.getAs[Array[Byte]]("value")
          pathInfo.getGUI_img.displayImage(imageBytes)
        }
      }
      .start()
    query_image.awaitTermination()

  }
}