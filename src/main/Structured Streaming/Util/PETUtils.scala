package Util

import PETLoader.PETLoader_Spark
import alg.StructuredStreaming01
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{BufferedWriter, FileWriter, PrintWriter}
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import java.util
import scala.collection.mutable.ListBuffer
//import test.Location.{LocationAnonymizer00, LocationAnonymizer01}
import scala.util.Try


class PETUtils extends Serializable {


//  private var sparkSession_all: SparkSession=_
//  object PipLineReconstruct{
//    private val BOOTSTRAP_SERVERS = "localhost:9092"
//    private var start_gps = true
//    private var start_img = false
//    private var start_location = false
//    def initialize(): Unit = {
//      val client = RedisUtil.getJedisClient
//      start_gps = client.get("start_gps").toBoolean
//      start_location = client.get("start_location").toBoolean
//      start_img = client.get("start_img").toBoolean
//      client.close()
//    }
//    private var df_1:DataFrame = _
//    private var df_2:DataFrame = _
//    def select(): (DataFrame,DataFrame) = {
//      println("zhixingla")
////      sparkSession_all = sparkSession
//      initialize()
//      println("start_gps: "+start_gps)
//      println("start_img: "+start_img)
//      if(start_gps){
//        println("Spark: "+spark)
//        df_1 = spark.readStream
//          .format("kafka")
//          .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
//          .option("subscribe", "test-data")
//          .load()
//      }
//      if(start_img){
//        df_2 = spark.readStream
//          .format("kafka")
//          .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
//          .option("subscribe", "test-image")
//          .load()
//      }
//      (df_1,df_2)
//    }
//  }


  object TakeSomeInfo extends MapFunction[String, SaveInfo_Java] {
    // 实现 call 方法
    override def call(line: String): SaveInfo_Java = {
      // 将 Row 转换为 alg.SelectEvent 对象
      val words: Array[String] = line.split(",")

      new SaveInfo_Java(words(0).toDouble,
        words(1).toDouble, words(2).toDouble,
        words(3).toDouble, words(12).toDouble,
        words(13).toDouble, words(9).toDouble
      )
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
    private var count = 0
    private val UserHome = new Tuple2[java.lang.Double, java.lang.Double](48.98561, 8.39571)

    def update(): Unit = {
      val client = RedisUtil.getJedisClient
      client.set("SPEED", "1")
      client.close()
    }

    override def call(saveInfo: SaveInfo_Java): SaveInfo_Java = {
      count += 1

      saveInfo.setPETPolicy("IMAGE", 1)
      println(count)

      saveInfo
    }
  }


  object Evaluation extends MapFunction[SaveInfo_Java, SaveInfo_Java] {
    private val UserHome = new Tuple2[java.lang.Double, java.lang.Double](48.98561, 8.39571)
    private val thredhold = 0.00135
    private var SPEED_PET_ID : Int= _
    private val Type_S = "SPEED"
    private var LOCATION_PET_ID : Int= _
    private val Type_L = "LOCATION"
    private var IMAGE_PET_ID : Int= _
    private val Type_I = "IMAGE"
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
      SPEED_PET_ID = client.get("SPEED").toInt
      LOCATION_PET_ID = client.get("LOCATION").toInt
      IMAGE_PET_ID = client.get("IMAGE").toInt
      client.close()
    }

    def update(Type:String,ID:Int): Unit = {
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
      if(SPEED_PET_ID==1){
        val client = RedisUtil.getJedisClient
        client.set("start_img","true")
        client.close()

        StructuredStreaming01.PipLineReconstruct.select()
      }
      if(saveInfo.getPosition != null){
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
      private val executionTimes_SPEED = ListBuffer[String]()
      private val executionTimes_LOCATION = ListBuffer[String]()
      private val executionTimes_IMAGE = ListBuffer[String]()
      private var fileWriter:FileWriter=_
      PETLoader = new PETLoader_Spark(confPath, Type, id)
      PETLoader.instantiate()
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

        Type match {
          case "SPEED" =>
            val invoke_speed: Double = PETLoader.invoke(saveInfo.getVel.asInstanceOf[T]).get(0).asInstanceOf[Double]
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
