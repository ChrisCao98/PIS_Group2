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


class PETUtils extends Serializable {




  object TakeSomeInfo extends MapFunction[String, SaveInfo_Java] {
    // 实现 call 方法
    override def call(line: String): SaveInfo_Java = {
      // 将 Row 转换为 alg.SelectEvent 对象
      val words: Array[String] = line.split(",")

      val saveInfo_Java: SaveInfo_Java = new SaveInfo_Java(words(0).toDouble,
        words(1).toDouble, words(2).toDouble,
        words(3).toDouble, words(12).toDouble,
        words(13).toDouble, words(9).toDouble
      )
      saveInfo_Java.recordTimer()
      saveInfo_Java
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

  object AddImageInfo extends MapFunction[Array[Byte], SaveInfo_Java] {

    override def call(array: Array[Byte]): SaveInfo_Java = {
      val saveInfo_Java: SaveInfo_Java = new SaveInfo_Java()
      saveInfo_Java.setImg(array)
      saveInfo_Java.recordTimer()
      saveInfo_Java
    }

  }




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

      saveInfo
    }
  }


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
//    var spark: SparkSession = null
//    def  setSpart(sparkSession: SparkSession): Unit = {
//      spark= sparkSession
//    }
//    println("count chushihua")
//    var count :Int = 0
//    println(count)
//    println("chushihua?")
//    initialize()
//    println("jieshu?")
    println(Type_S,SPEED_PET_ID)
    println(Type_L,LOCATION_PET_ID)
    println(Type_I,IMAGE_PET_ID)

    def initialize(): Unit ={
      val client = RedisUtil.getJedisClient

      SPEED_PET_ID = client.get("SpeedPET").toInt
      IMAGE_PET_ID = client.get("CameraPET").toInt
      println("IMAGE_PET_ID: "+IMAGE_PET_ID)
//      println("<<<<<>>>>>")
//      println(client.get("SpeedSituation"))
//      println(client.get("SpeedSituation").toInt)
//      println("<<<<<>>>>>")
//      SpeedSituation = client.get("SpeedSituation").toBoolean
      SpeedSituation = if (client.get("SpeedSituation") == "0") false else true
      println("SpeedSituation: "+SpeedSituation)
      LocationSituation = if (client.get("LocationSituation") == "0") false else true
      CameraSituation = if (client.get("CameraSituation") == "0") false else true
      client.close()
    }

    def update(Type:String,ID:Int): Unit = {
      val client = RedisUtil.getJedisClient
      client.set(Type, ID.toString)
      Type match {

        case "SPEED" => SPEED_PET_ID = client.get("SpeedPET").toInt
        case "LOCATION" => LOCATION_PET_ID = client.get("LocationPET").toInt
        case "IMAGE" => IMAGE_PET_ID = client.get("CameraPET").toInt
      }
      client.close()
    }


    override def call(saveInfo: SaveInfo_Java): SaveInfo_Java = {
      initialize()

//      if(SPEED_PET_ID==1){
//        println("qie huan")
//        val client = RedisUtil.getJedisClient
//        client.set("start_img","true")
//        client.close()
//        //        stopQuery("test-image")
////        SS03.addQuery("test-image")
////        StructuredStreaming01.PipLineReconstruct.select()
//      }

      if (!SpeedSituation) {
        saveInfo.setPETPolicy(Type_S, 0)
      } else {
        saveInfo.setPETPolicy(Type_S, SPEED_PET_ID)
      }

      if (!LocationSituation) {
        saveInfo.setPETPolicy(Type_L, 0)
      } else {
        if (saveInfo.getPosition != null) {
          val distance = MathUtils.calculateDistance(UserHome, saveInfo.getPosition)
          val locationPET: Int = if (distance < thredhold) 1 else 0
          if (locationPET != LOCATION_PET_ID) {
            println("Policy changed from " + LOCATION_PET_ID + " to " + locationPET)
            update(Type_L, locationPET)
          }
          saveInfo.setPETPolicy(Type_L, LOCATION_PET_ID)

        }
      }

      if (!CameraSituation) {
        saveInfo.setPETPolicy(Type_I, 0)
      } else {
        saveInfo.setPETPolicy(Type_I, IMAGE_PET_ID)
      }

      saveInfo.recordTimer()
      saveInfo
    }
  }

  import org.apache.commons.csv.{CSVFormat, CSVPrinter}

  object ApplyPET extends Serializable {
    def apply[T](confPath: String, Type: String): ApplyPET[T] = {
      new ApplyPET[T](confPath, Type)
    }


    class ApplyPET[T](var confPath: String, var Type: String) extends MapFunction[SaveInfo_Java, SaveInfo_Java] {
      private var id: Integer = 0
      private var PETLoader: PETLoader_Spark[T] = _
      private var startTime: Long = _
      private var endTime: Long = _
      private var executionTime:String = _
      private var ss :String = _
      println("zhixngjici")
      private val executionTimes_SPEED = ListBuffer[String]()
      private val executionTimes_LOCATION = ListBuffer[String]()
      private val executionTimes_IMAGE = ListBuffer[String]()
      private var fileWriter:FileWriter=_
      PETLoader = new PETLoader_Spark(confPath, Type, id)
      PETLoader.instantiate()
      println("ceshi")
//      println(PETLoader.getSparkSession)
      //    println("***********************")
      //    println(PETLoader.getSize)

      //       Reload PET method
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
            fileWriter= new FileWriter("./data/execution_times_SPEED.txt", true)
          case "LOCATION"=>
            println("??????write in?LOCATION")
            fileWriter = new FileWriter("./data/execution_times_LOCATION.txt", true)
          case "IMAGE" =>
            fileWriter = new FileWriter("./data/execution_times_IMAGE.txt", true)
        }
        val bufferedWriter = new BufferedWriter(fileWriter)
        val writer = new PrintWriter(bufferedWriter)
        writer.println(ss)
        writer.close()
      }

      override def call(saveInfo: SaveInfo_Java): SaveInfo_Java = {
        if (id ne saveInfo.getPETPolicy.get(Type)) {
          ss = Type +" Switch Policy from "+id+" to "

          id = saveInfo.getPETPolicy.get(Type)
          reloadPET()

        }
        saveInfo.recordTimer()

        Type match {
          case "SPEED" =>
//            if(aaaa){
//              println("start")
//              SS03.addQuery("test-image")
//              println("finish")
//            }

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
