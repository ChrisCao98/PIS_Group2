//package alg
//
//import org.apache.spark.sql._
//
//import java.sql.Timestamp
//
//
//
//object democsv {
//
//
//  def main(args: Array[String]): Unit = {
//    val path: String = "config/Pipeconfig.json"
//    val pathInfo = alg.PathInfo(path)
////    val jarDir = "/home/chriscao/IdeaProjects/kfaka_no_gui/lib"
////    val jars = new File(jarDir).listFiles.filter(_.getName.endsWith(".jar")).map(_.getPath)
////    val conf = new SparkConf()
////      .setJars(jars)
//
//    // 创建 SparkSession
//    val spark = SparkSession.builder()
////      .config(conf)
//      .appName("CSV Streaming Demo")
//      .master("local[*]")
//      .getOrCreate()
////    spark.conf.set("spark.default.parallelism", "1")
//
//    implicit val saveInfoEncoder: Encoder[Util.SaveInfo_Java] = Encoders.javaSerialization[Util.SaveInfo_Java]
//    import spark.implicits._
//
//    val gpsSchema = new GPSSchema().schema
//    //read gpsdata
//    val streamingData = spark.readStream
//      .format("csv")
//      .option("delimiter", ",")
//      .schema(gpsSchema)
//      .load("/home/chriscao/IdeaProjects/data/csvdata")
//
//    //parse for gpsdata
//    val gpsDataset = streamingData.as[alg.GPSEvent]
//    val seleDataSet:Dataset[Util.SaveInfo_Java]=gpsDataset.map(new Util.PETUtils().TakeSomeInfo, saveInfoEncoder)
//
//    val evaDataSet:Dataset[Util.SaveInfo_Java] = seleDataSet.map(new Util.PETUtils().EvaluationData, saveInfoEncoder)
//
//
//
//    val applyDetaSet:Dataset[Util.SaveInfo_Java] = evaDataSet
//      .map(new Util.PETUtils().ApplyPET(pathInfo.getPETconfpath, "SPEED"),saveInfoEncoder)
////      .map(new petUtils.ApplyPET(pathInfo.getPETconfpath, "LOCATION"),saveInfoEncoder)
//
//    val finalDataSet:Dataset[alg.FinalGPSEvent] = applyDetaSet.map { Util.SaveInfo_Java
//    =>
//      alg.FinalGPSEvent(new Timestamp(Util.SaveInfo_Java.getTimestamp.toLong),
//        Util.SaveInfo_Java.getLocation.asScala.toList,
//        Util.SaveInfo_Java.getAltitude,
//        Util.SaveInfo_Java.getAcc_x,
//        Util.SaveInfo_Java.getAcc_y,
//        Util.SaveInfo_Java.getVel
//      )
//    }
//
//
////    val query: StreamingQuery = finalDataSet.writeStream
////      .outputMode("update")
////      .trigger(Trigger.ProcessingTime("1 second"))
////      .format("console")
////      .option("maxFilesPerTrigger", 5)
////      .start()
////
////    query.awaitTermination()
//
//    val query = finalDataSet.toDF().writeStream
//      .outputMode("append")
//      .foreachBatch { (batchDF: Dataset[Row], _: Long) =>
//        batchDF.show(25, false)
//      }
//      .start()
//
//    query.awaitTermination()
//  }
//}
//
