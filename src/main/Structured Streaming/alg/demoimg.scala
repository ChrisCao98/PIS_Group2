package alg


import Util.{GUI, PETUtils, SaveInfo_Java}
import org.apache.spark.sql._

import java.io.{ByteArrayOutputStream, File}
import javax.imageio.ImageIO

/**
 * This class is mainly to test a set of processes for images, before this there should be a set of processes for testing GPS data,
 * but unfortunately I accidentally deleted it. In this class is mainly completed for the picture of the reading, processing,
 * and save to the specified folder process.
*/
object demoimg {
  val GUI_img = new GUI()

  // read image data
  def readImage(filePath: String): Array[Byte] = {
    val imageFile = new File(filePath)
    val bufferedImage = ImageIO.read(imageFile)
    val outputStream = new ByteArrayOutputStream()
    ImageIO.write(bufferedImage, "jpg", outputStream)
    outputStream.toByteArray
  }

  def main(args: Array[String]): Unit = {
    val path: String = "config/Pipeconfig.json"
    val pathInfo = PathInfo(path)
    val spark = SparkSession.builder().appName("ImageProcessing").master("local[*]").getOrCreate()
    import spark.implicits._
    implicit val saveInfoEncoder: Encoder[SaveInfo_Java] = Encoders.javaSerialization[SaveInfo_Java]


    val df_img = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      //      .option("subscribe", "test-data,test-image,test-user-input")
      .option("subscribe", "test-image")
      .load()
      .select("value")
      .as[Array[Byte]]



    val seleDataSet:Dataset[SaveInfo_Java]=df_img.map(new PETUtils().AddImageInfo, saveInfoEncoder)

    val evaDataSet:Dataset[SaveInfo_Java] = seleDataSet.map(new PETUtils().EvaluationImage, saveInfoEncoder)

    val applyDetaSet: Dataset[SaveInfo_Java] = evaDataSet
      //      .map(new petUtils.ApplyPET(pathInfo.getPETconfpath, "SPEED"),saveInfoEncoder)
      .map(new PETUtils().ApplyPET(pathInfo.getPETconfpath, "IMAGE"), saveInfoEncoder)
    val ff = applyDetaSet.map{
      saveInfo_Java=>
        saveInfo_Java.getImg
    }.toDF()


    // Save image data to a specified folder
    val outputPath = "/home/chriscao/IdeaProjects/kfaka_no_gui/src/main/resources/testImage"
    val query = ff
      .writeStream
      .foreachBatch { (batchDF:DataFrame, batchId:Long) =>
        batchDF.foreach { row =>
          val imageBytes = row.getAs[Array[Byte]]("value")
          GUI_img.displayImage(imageBytes)
        }
      }
      .start()
      .awaitTermination()

  }


}
