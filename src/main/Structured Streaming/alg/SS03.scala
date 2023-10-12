package alg

import Util.{MathUtils, PETUtils, RedisUtil, SaveInfo_Java}
import alg.SS03.pathInfo
import alg.StructuredStreaming.{BOOTSTRAP_SERVERS, initialize, pathInfo, spark}
import org.apache.kafka.common.internals.Topic
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}

import java.sql.Timestamp
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
/**
 * Here is the third variation for StructuredStreaming.
 * The difference between object StructuredStreaming03 and object StructuredStreaming is that i have tried to dynamically
 * add and remove stream.
 * I use a list to record the name of topic and a hashmap to write the corresponding address of stream.
 * The disadvantage of the method is for each topic should write a code.
 */
object SS03 {
  private val BOOTSTRAP_SERVERS = "localhost:9092"
  val path: String = "config/Pipeconfig.json"
  val pathInfo = PathInfo(path)
  initialize()
  val hashMap = new mutable.HashMap[String, StreamingQuery]()
  val queries = scala.collection.mutable.ArrayBuffer.empty[StreamingQuery]

  var topics: Seq[String] = Seq("test-data", "test-image", "test-user-input")
//    var topics: Seq[String] = Seq("test-data", "test-user-input")
  def getTopics(): Seq[String] = {
    topics
  }

  def getQueries()= {
    queries
  }


  def initialize(): Unit = {
    val client = RedisUtil.getJedisClient
    client.set("SpeedPET", "0")
    client.set("LocationPET", "0")
    client.set("CameraPET", "0")
    client.set("CameraSituation", "0")
    client.set("LocationSituation", "0")
    client.set("SpeedSituation", "0")

    /**
     * Test dynamically adding or removing streams
     */
//    client.set("CameraSituation", "1")
//    client.set("LocationSituation", "1")
//    client.set("SpeedSituation", "1")
//    client.set("Dynamic", "1")

    client.close()
  }


  val spark: SparkSession = SparkSession
    .builder()
    .appName("Streaming process from Kafka")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.default.parallelism", "1")
    .getOrCreate()

  def getSpark(): SparkSession = {
    spark
  }

  import spark.implicits._
  implicit val saveInfoEncoder: Encoder[SaveInfo_Java] = Encoders.javaSerialization[SaveInfo_Java]
  def load(TopicName: String): DataFrame = {
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
      .option("subscribe", TopicName)
      .load()
    df
  }

  /**
   * This function is designed to remove a stream. The input is the name of topic.
   */
  def stopQuery(name: String): Unit = {
    if(topics.contains(name))
      println(name)
      println(topics)
      val query = hashMap(name)
      query.stop()
      hashMap.remove(name)
      queries -= query
      println(queries)
      topics = topics.filter(_ != "test-image")
      println(topics)
  }

  /**
   * This function is designed to add a stream. The input is the name of topic.
   */
  def addQuery(newtopic: String):Unit = {
    if (!topics.contains(newtopic) && newtopic.equals("test-image")) {
      val query = queryForImage("test-image")
      queries += query
      topics = topics :+ "test-image"
      println(topics)
      println(queries)
      hashMap.put(newtopic, query)
    }
  }

  /**
   * This function is designed to remove all the streams.
   */
  def stopAll(): Unit = {
    for (query <- queries) {
      query.stop()
    }
  }

  /**
   * This function is designed for GPS data.
   */
  def queryForGPS(topic: String): StreamingQuery = {
    val query = load(topic)
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
      .toDF()
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false) // 可选：显示完整的列内容
      //      .trigger(Trigger.Continuous("10 milliseconds"))
      .start()
    query
  }

  /**
   * This function is designed for image data.
   */
  def queryForImage(topic: String): StreamingQuery = {
    val query = load(topic).select("value")
      .as[Array[Byte]]
      .map(new PETUtils().AddImageInfo, saveInfoEncoder)
      .map(new PETUtils().EvaluationImage, saveInfoEncoder)
      .map(new PETUtils().ApplyPET(pathInfo.getPETconfpath, "IMAGE"), saveInfoEncoder)
      .map {
        saveInfo_Java =>
          saveInfo_Java.getImg
      }.toDF()
      .writeStream
      //      .foreachBatch(new ForeachWriter[Array[Byte]] {})
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.foreach { row =>
          val imageBytes = row.getAs[Array[Byte]]("value")
          pathInfo.getGUI_img.displayImage(imageBytes)
        }
      }
      .start()
    query
  }

  def main(args: Array[String]): Unit = {
    for (topic <- topics) {
      if (topic.equals("test-data")) {
        var query = queryForGPS(topic)
        queries += query
        hashMap.put(topic, query)
      } else if (topic.equals("test-image")) {
        val query = queryForImage("test-image")
        queries += query
        hashMap.put(topic, query)
      }
    }
    for (query <- queries) {
      println("zhixing jibian")
      println("query: "+query)
      println("queries: "+queries)

      query.awaitTermination()
    }
  }



  object Evaluation extends MapFunction[SaveInfo_Java, SaveInfo_Java] {
    private val UserHome = new Tuple2[java.lang.Double, java.lang.Double](48.98561, 8.39571)
    private val thredhold = 0.00135
    private var SPEED_PET_ID: Int = _
    private val Type_S = "SPEED"
    private var LOCATION_PET_ID: Int = _
    private val Type_L = "LOCATION"
    private var IMAGE_PET_ID: Int = _
    private val Type_I = "IMAGE"
    println(Type_S, SPEED_PET_ID)
    println(Type_L, LOCATION_PET_ID)
    println(Type_I, IMAGE_PET_ID)

    def initialize(): Unit = {
      val client = RedisUtil.getJedisClient
      SPEED_PET_ID = client.get("SPEED").toInt
      LOCATION_PET_ID = client.get("LOCATION").toInt
      IMAGE_PET_ID = client.get("IMAGE").toInt
      client.close()
    }

    def update(Type: String, ID: Int): Unit = {
      val client = RedisUtil.getJedisClient
      client.set(Type, ID.toString)
      Type match {
        case "SPEED" => SPEED_PET_ID = client.get("SPEED").toInt
        case "LOCATION" => LOCATION_PET_ID = client.get("LOCATION").toInt
        case "IMAGE" => IMAGE_PET_ID = client.get("IMAGE").toInt
      }
      client.close()
    }


    override def call(saveInfo: SaveInfo_Java): SaveInfo_Java = {
      initialize()
      println("******************")
      if (SPEED_PET_ID == 1) {
        val client = RedisUtil.getJedisClient
        client.set("start_img", "false")
        client.close()
        println("????????????????????")
//        stopQuery("test-image")
        addQuery("test-image")
      }
      if (saveInfo.getPosition != null) {
        val distance = MathUtils.calculateDistance(UserHome, saveInfo.getPosition)
        val locationPET: Int = if (distance < thredhold) 1 else 0
        if (locationPET != LOCATION_PET_ID) {
          println("Policy changed from " + LOCATION_PET_ID + " to " + locationPET)
          update(Type_L, locationPET)
        }
        saveInfo.setPETPolicy(Type_L, LOCATION_PET_ID)

      }
      saveInfo.setPETPolicy(Type_S, SPEED_PET_ID)
      saveInfo.setPETPolicy(Type_I, IMAGE_PET_ID)
      saveInfo
    }
  }
}
