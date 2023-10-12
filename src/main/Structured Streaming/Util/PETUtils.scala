package Util

import PETLoader.PETLoader_Spark
import alg.{SS03, StructuredStreaming01}
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import java.{lang, util}
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ListBuffer
//import test.Location.{LocationAnonymizer00, LocationAnonymizer01}
import scala.util.Try

/**
 * This class holds the main tools.
 */
class PETUtils extends Serializable {
  /**
   * This object mainly extracts the needed information and wraps it into an object.
   */
  object TakeSomeInfo extends MapFunction[String, SaveInfo_Java] {

    override def call(line: String): SaveInfo_Java = {

      val words: Array[String] = line.split(",")

      val saveInfo_Java: SaveInfo_Java = new SaveInfo_Java(words(0).toDouble,
        words(1).toDouble, words(2).toDouble,
        words(3).toDouble, words(12).toDouble,
        words(13).toDouble, words(9).toDouble
      )
      saveInfo_Java.recordTimer()
      saveInfo_Java

      //The following code is used for testing.
//      val selectEvent = alg.SelectEvent(
//        gpsEvent.timestamp,
//        gpsEvent.lat,
//        gpsEvent.lon,
//        gpsEvent.alt,
//        gpsEvent.ax,
//        gpsEvent.ay,
//        gpsEvent.vf
//      )

      // 创建 Util.SaveInfo_Java 对象并返回
//      new Util.SaveInfo_Java(
//        selectEvent.timestamp,
//        selectEvent.lat,
//        selectEvent.lon,
//        selectEvent.alt,
//        selectEvent.ax,
//        selectEvent.ay,
//        selectEvent.vf
//      )
    }
  }

  /**
   * This object wraps image data into an object.
   */
  object AddImageInfo extends MapFunction[Array[Byte], SaveInfo_Java] {

    override def call(array: Array[Byte]): SaveInfo_Java = {
      val saveInfo_Java: SaveInfo_Java = new SaveInfo_Java()
      saveInfo_Java.setImg(array)
      saveInfo_Java.recordTimer()
      saveInfo_Java
    }

  }


  /**
   * This is a test object for gps data.
   */
  object EvaluationData extends MapFunction[SaveInfo_Java, SaveInfo_Java] {
    @volatile private var count = 0
    private val UserHome = new Tuple2[java.lang.Double, java.lang.Double](48.98561, 8.39571)
    private val thredhold = 0.00135

    def update(): Unit = {
      val client = RedisUtil.getJedisClient
      client.set("SPEED","1")
      client.close()
    }
    override def call(saveInfo: SaveInfo_Java): SaveInfo_Java = {
      update()
      count += 1

      val distance = MathUtils.calculateDistance(UserHome, saveInfo.getPosition)
      val locationPET: Int = if (distance < thredhold) 1 else 0

      saveInfo.setPETPolicy("LOCATION", locationPET)

      if (count > 10) {
        saveInfo.setPETPolicy("SPEED", 1) // 根据条件更新属性值
        //        saveInfo.setPETPolicy("LOCATION", 1) // 更新属性值
      }
      saveInfo.setPETPolicy("IMAGE", 1)
      println(count)

      saveInfo
    }
  }

  /**
   * This is a test object for image data.
   */
  object EvaluationImage extends MapFunction[SaveInfo_Java, SaveInfo_Java] {
    private val UserHome = new Tuple2[java.lang.Double, java.lang.Double](48.98561, 8.39571)
    private val thredhold = 0.00135
    private var CameraSituation: Boolean = _

    private var IMAGE_PET_ID: Int = _
    private val Type_I = "IMAGE"

    def initialize(): Unit = {
      val client = RedisUtil.getJedisClient
      IMAGE_PET_ID = client.get("CameraPET").toInt
      CameraSituation = client.get("CameraSituation").toBoolean
      client.close()
    }
    def update(): Unit = {
      val client = RedisUtil.getJedisClient
      client.set("SPEED", "1")
      client.close()
    }

    override def call(saveInfo: SaveInfo_Java): SaveInfo_Java = {

      saveInfo.setPETPolicy("IMAGE", 1)
      saveInfo.recordTimer()
      saveInfo
    }
  }

  /**
   * This class is used to evaluate which PET should be used.
   */
  object Evaluation extends MapFunction[SaveInfo_Java, SaveInfo_Java] {
    private val UserHome = new Tuple2[java.lang.Double, java.lang.Double](48.98561, 8.39571)
    private val thredhold = 0.00135
    private var SPEED_PET_ID : Int= _
    private val Type_S = "SpeedPET"
    private var CameraSituation :Boolean = _
    private var LocationSituation :Boolean = _
    private var SpeedSituation :Boolean = _

    private var LOCATION_PET_ID : Int= _
    private val Type_L = "LocationPET"
    private var IMAGE_PET_ID : Int= _
    private val Type_I = "CameraPET"

    private var dynamic : String = _

    /**
     * This function mainly initialises the values of some member variables.
     */
    def initialize(): Unit ={
      val client = RedisUtil.getJedisClient

      SPEED_PET_ID = client.get("SpeedPET").toInt
      IMAGE_PET_ID = client.get("CameraPET").toInt
      println("IMAGE_PET_ID: "+IMAGE_PET_ID)

      SpeedSituation = if (client.get("SpeedSituation") == "0") false else true
      println("SpeedSituation: "+SpeedSituation)
      LocationSituation = if (client.get("LocationSituation") == "0") false else true
      CameraSituation = if (client.get("CameraSituation") == "0") false else true
      dynamic = client.get("Dynamic")
      client.close()
    }
    /**
     * This function is used to update the ID value of the PET.
     */
    def update(Type:String,ID:Int): Unit = {
      val client = RedisUtil.getJedisClient
      client.set(Type, ID.toString)


      Type match {

        case "SpeedPET" =>
          SPEED_PET_ID = client.get("SpeedPET").toInt
        case "LocationPET" =>
          LOCATION_PET_ID = client.get("LocationPET").toInt
        case "CameraPET" =>
          IMAGE_PET_ID = client.get("CameraPET").toInt
      }
      client.close()
    }

    /**
     * This function determines what the final PET ID will be.
     */
    override def call(saveInfo: SaveInfo_Java): SaveInfo_Java = {
      initialize()

//      if(dynamic == "0"){
//        println("***********")
//        var starttime :Long = System.nanoTime()
//        println("qie huan")
//        val client = RedisUtil.getJedisClient
//        client.set("Dynamic","1")
////        client.close()
//
//        //test for remove stream
//        SS03.stopQuery("test-image")
//
//        //test for add stream
////        SS03.addQuery("test-image")
////        StructuredStreaming01.PipLineReconstruct.select()
//        var endtime :Long = System.nanoTime()
//        var duration_add = (endtime-starttime).toString
////        var fileWriter = new FileWriter("./data/switch_add.txt", true)
//        var fileWriter = new FileWriter("./data/switch_remove.txt", true)
//        val bufferedWriter = new BufferedWriter(fileWriter)
//        val writer = new PrintWriter(bufferedWriter)
//        writer.println(duration_add)
//        writer.close()
////        println("***************************")
////        println("duration: "+(endtime-starttime))
////        println("***************************")
//      }
      println("situation "+SpeedSituation)
      println("situation "+LocationSituation)
      println("situation "+CameraSituation)

      if (!SpeedSituation) {
        saveInfo.setPETPolicy("SPEED", 0)
      } else {
        saveInfo.setPETPolicy("SPEED", SPEED_PET_ID)

      }

      if (!LocationSituation) {
        saveInfo.setPETPolicy("LOCATION", 0)
      } else {
        println("********"+saveInfo.getLocation)
        if (saveInfo.getLocation != null) {
          val distance = MathUtils.calculateDistance(UserHome, saveInfo.getPosition)
          val locationPET: Int = if (distance < thredhold) 1 else 0
          if (locationPET != LOCATION_PET_ID) {
            println("Policy changed from " + LOCATION_PET_ID + " to " + locationPET)
            update(Type_L, locationPET)
          }
          saveInfo.setPETPolicy("LOCATION", LOCATION_PET_ID)

        }
      }

      if (!CameraSituation) {
        saveInfo.setPETPolicy("IMAGE", 0)
        println("c policy: 0")
      } else {
        //for test,1 can be replaced by IMAGE_PET_ID
        saveInfo.setPETPolicy("IMAGE", 1)
        println("c policy: 1")
      }

      saveInfo.recordTimer()
      saveInfo
    }
  }

  import org.apache.commons.csv.{CSVFormat, CSVPrinter}
  /**
   * This object is used to switch the PET and process the incoming data using the corresponding algorithm.
   */
  object ApplyPET extends Serializable {
    def apply[T](confPath: String, Type: String): ApplyPET[T] = {
      new ApplyPET[T](confPath, Type)
    }

    /**
     * This class is the part that ends up manipulating the data.
     */
    class ApplyPET[T](var confPath: String, var Type: String) extends MapFunction[SaveInfo_Java, SaveInfo_Java] {
      private var id: Integer = 0
      private var PETLoader: PETLoader_Spark[T] = _
      private var startTime: Long = _
      private var endTime: Long = _
      private var executionTime:String = _
      private var ss :String = _
//      private val executionTimes_SPEED = ListBuffer[String]()
//      private val executionTimes_LOCATION = ListBuffer[String]()
//      private val executionTimes_IMAGE = ListBuffer[String]()
      private var fileWriter:FileWriter=_
      //Initialise PETLoader
      PETLoader = new PETLoader_Spark(confPath, Type, id)
      PETLoader.instantiate()
//      println(PETLoader.getSparkSession)
      //    println("***********************")
      //    println(PETLoader.getSize)



      /**
       * This function is used for switching the PET
       */
      private def reloadPET(): Try[Unit] = Try {
        startTime= System.currentTimeMillis()

        PETLoader.reloadPET(id)
        PETLoader.instantiate()

        endTime= System.currentTimeMillis()
//        endTime = System.nanoTime()
        executionTime = Duration(endTime - startTime, TimeUnit.MILLISECONDS).toString()
        ss = ss + id+","+executionTime

        Type match {
          case "SPEED" =>
            println("??????write in?SPEED")
            fileWriter= new FileWriter("./data/ChangePET_times_SPEED.txt", true)
          case "LOCATION"=>
            println("??????write in?LOCATION")
            fileWriter = new FileWriter("./data/ChangePET_times_LOCATION.txt", true)
          case "IMAGE" =>
            fileWriter = new FileWriter("./data/ChangePET_times_IMAGE.txt", true)
        }
        val bufferedWriter = new BufferedWriter(fileWriter)
        val writer = new PrintWriter(bufferedWriter)
        writer.println(ss)
        writer.close()
      }
      /**
       * This function is a call to the algorithm to manipulate the data.
       */
      override def call(saveInfo: SaveInfo_Java): SaveInfo_Java = {
        //If the id is different from the previous one. Then the PET has to be reloaded.
        if (id ne saveInfo.getPETPolicy.get(Type)) {
          ss = Type +" Switch Policy from "+id+" to "

          id = saveInfo.getPETPolicy.get(Type)
          reloadPET()

        }
        saveInfo.recordTimer()
//        println("size:" + saveInfo.getTimerRecord.size())
        Type match {
          case "SPEED" =>
            val invoke_speed = PETLoader.invoke(saveInfo.getVel.asInstanceOf[T]).get(0).asInstanceOf[Double]
            saveInfo.setVel(invoke_speed)
          case "LOCATION" =>
            val invoke_pos: util.ArrayList[(java.lang.Double, java.lang.Double)] = PETLoader.invoke(saveInfo.getPosition.asInstanceOf[T])
              .asInstanceOf[util.ArrayList[(java.lang.Double, java.lang.Double)]]
            saveInfo.setLocation(invoke_pos)
          case "IMAGE" =>
            val invoke_img: Array[Byte] = PETLoader.invoke(saveInfo.getImg.asInstanceOf[T]).get(0).asInstanceOf[Array[Byte]]
            saveInfo.setImg(invoke_img)
          case _ =>
            throw new IllegalStateException("Unexpected value: " + Type)
        }


        saveInfo
      }
    }
  }
}
