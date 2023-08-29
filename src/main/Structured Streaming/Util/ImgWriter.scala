package Util

//import alg.PathInfo
import alg.StructuredStreaming.pathInfo
//import org.apache.commons.csv.{CSVFormat, CSVPrinter}
//import org.apache.spark.sql.ForeachWriter
//
//import java.io.{BufferedWriter, File, FileWriter}
//import java.{lang, util}
//import scala.collection.JavaConverters.asScalaBufferConverter


class ImgWriter extends CommonWriter("/home/chriscao/IdeaProjects/PIS_Group2/data/img.csv") {
//  super.filePath = "/home/chriscao/IdeaProjects/PIS_Group2/data/img.csv"
  override def process(saveInfo: SaveInfo_Java): Unit = {
    super.process(saveInfo)

    val imageBytes = saveInfo.getImg
    if(imageBytes != null){
      pathInfo.getGUI_img.displayImage(imageBytes)
    }
  }
//    override def process(saveInfo: SaveInfo_Java): Unit = {
//      saveInfo.recordTimer()
//      val imageBytes = saveInfo.getImg
//      val list: util.ArrayList[lang.Long] = saveInfo.getTimerRecord
//      println(list.size())
//
//      try {
//        if (new File(filePath).exists()) {
//          csvPrinter.println() // 插入一个空白行
//          csvPrinter.flush()
//        }
//
//        list.asScala.foreach { element =>
//          csvPrinter.print(element.toString) // 逐个打印每个元素
//        }
//
//      }
//      pathInfo.getGUI_img.displayImage(imageBytes)
//    }
}
//class ImgWriter extends ForeachWriter[SaveInfo_Java]{
//
//  val filePath = "/home/chriscao/IdeaProjects/PIS_Group2/data/img.csv"
//  var writer: BufferedWriter = _
//  var csvPrinter: CSVPrinter = _
//  override def open(partitionId: Long, epochId: Long): Boolean = {
//    writer = new BufferedWriter(new FileWriter(filePath, true))
//    csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)
//    writer != null && csvPrinter != null
//  }
//
//  override def process(saveInfo: SaveInfo_Java): Unit = {
//    saveInfo.recordTimer()
//    val imageBytes = saveInfo.getImg
//    val list: util.ArrayList[lang.Long] = saveInfo.getTimerRecord
//    println(list.size())
//
//    try {
//      if (new File(filePath).exists()) {
//        csvPrinter.println() // 插入一个空白行
//        csvPrinter.flush()
//      }
//
//      list.asScala.foreach { element =>
//        csvPrinter.print(element.toString) // 逐个打印每个元素
//      }
//
//    }
//    pathInfo.getGUI_img.displayImage(imageBytes)
//  }
//
//  override def close(errorOrNull: Throwable): Unit = {
//    if (writer != null && csvPrinter != null) {
//      csvPrinter.close()
//      writer.close()
//    }
//  }
//}
