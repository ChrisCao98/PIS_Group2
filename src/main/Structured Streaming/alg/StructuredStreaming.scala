package alg
import alg.config.PropertiesConfig
import Util.{CsvWriter, ImgWriter, PETUtils, RedisUtil, RedisWriter, SaveInfo_Java}
import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}

import java.io.{BufferedWriter, File, FileWriter}
import java.{lang, util}
import java.sql.Timestamp
import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * This is the original stream process for this project.
 * In this class, I assume that I have three types of data, each consumed from kafka for separate processing.
 */
object StructuredStreaming {
  private val BOOTSTRAP_SERVERS = PropertiesConfig.getProperty("kafka.bootstrap.servers", "localhost:9092")
  private val configPath = PropertiesConfig.getProperty("pipe.config.path", "config/Pipeconfig.json")
  val pathInfo = PathInfo(configPath)
  initialize()

  /**
   * There are six types of data here, corresponding to the digits of the PET and the switching signals that process the data.
   */
  def initialize(): Unit = {
    val client = RedisUtil.getJedisClient
    client.set("SpeedPET", "0")
    client.set("LocationPET", "0")
    client.set("CameraPET", "0")
//    client.set("CameraSituation", "0")
//    client.set("LocationSituation", "0")
//    client.set("SpeedSituation", "0")

    //for test
    client.set("CameraSituation", "1")
    client.set("LocationSituation", "1")
    client.set("SpeedSituation", "1")
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

  //This function load data from the topic of Kafka.
  def load(TopicName:String):DataFrame = {
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
      .option("subscribe", TopicName)
      .load()
    df
  }



//  private var a = "false"

  /**
   * This function handle the data. Here i use 3 topics"test-data,test-image,test-user-input" to get data from Kafka.
   * And for each dataframe may use different way to process it. For example, for GPS data, first select the required data,
   * transform the Dataframe into a DataSet, and then apply the written methods to manipulate the data.
   *
  */
  def main(args: Array[String]): Unit = {

    implicit val saveInfoEncoder: Encoder[SaveInfo_Java] = Encoders.javaSerialization[SaveInfo_Java]

    //load data

    val df_gps = load("test-data")
    val df_image = load("test-image")
    val df_input = load("test-user-input")

    //process user input
    val DS_User = df_input
      .selectExpr("CAST(value AS STRING)")
      .as[String]

//    process GPS data
    val Ds_GPS = df_gps
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(new PETUtils().TakeSomeInfo, saveInfoEncoder)
      .map(new PETUtils().Evaluation, saveInfoEncoder)
      .map(new PETUtils().ApplyPET(pathInfo.getPETconfpath, "SPEED"),saveInfoEncoder)
      .map(new PETUtils().ApplyPET(pathInfo.getPETconfpath, "LOCATION"),saveInfoEncoder)
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
    //process image
    val DS_Image = df_image
      .select("value")
      .as[Array[Byte]]
      .map(new PETUtils().AddImageInfo, saveInfoEncoder)
//      .map(new PETUtils().EvaluationImage, saveInfoEncoder)
      .map(new PETUtils().Evaluation, saveInfoEncoder)
      .map(new PETUtils().ApplyPET(pathInfo.getPETconfpath, "IMAGE"), saveInfoEncoder)
      .map {
        saveInfo_Java =>
          saveInfo_Java.getImg
      }
      .toDF()



    val query_gps = Ds_GPS.toDF()
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false) // 可选：显示完整的列内容
//      .trigger(Trigger.Continuous("10 milliseconds"))
      .start()

//    val query_gps = Ds_GPS
//      .writeStream
//      .foreach(new CsvWriter)
//      .start()

//    val query_img = DS_Image
//      .writeStream
//      .foreach(new ImgWriter)
//      .start()


    //for image data

        val query_img = DS_Image
          .writeStream
          .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
            batchDF.foreach { row =>
              println("paole?")
              val imageBytes = row.getAs[Array[Byte]]("value")
              pathInfo.getGUI_img.displayImage(imageBytes)
            }
          }
          .start()

//    val query_img = DS_Image
//      .writeStream
////      .foreachBatch(new ForeachWriter[Array[Byte]] {})
//      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
//        batchDF.foreach { row =>
//          val saveInfo_Java = row.getAs[SaveInfo_Java]("value")
//          val imageBytes = saveInfo_Java.getImg
//          val filePath = "/home/chriscao/IdeaProjects/PIS_Group2/data/img.csv"
//          val writer: BufferedWriter = new BufferedWriter(new FileWriter(filePath, true))
//          val csvPrinter: CSVPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)
//          saveInfo_Java.recordTimer()
//          val list: util.ArrayList[lang.Long] = saveInfo_Java.getTimerRecord
//          println(list.size())
//
//          try {
//            if (new File(filePath).exists()) {
//              csvPrinter.println()
//              csvPrinter.flush()
//            }
//
//            list.asScala.foreach { element =>
//              csvPrinter.print(element.toString)
//            }
//
//          }
//
//          csvPrinter.close()
//          writer.close()
//
//          pathInfo.getGUI_img.displayImage(imageBytes)
//        }
//      }
//      .start()

    val query_user = DS_User
      .writeStream
      .foreach(new RedisWriter)
      .start()

    println("query_gps.id: "+query_gps.id)
    println("query_img.id: "+query_img.id)
    println("query_user.id: "+query_user.id)


    query_gps.awaitTermination()
    query_img.awaitTermination()
    query_user.awaitTermination()

  }
}
