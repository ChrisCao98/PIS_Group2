package Util

import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.apache.spark.sql.ForeachWriter

import java.io.{BufferedWriter, File, FileWriter}
import java.{lang, util}
import scala.collection.JavaConverters.asScalaBufferConverter

class CommonWriter(filePath: String) extends ForeachWriter[SaveInfo_Java]{
//  var filePath: String = _
  var writer: BufferedWriter = _
  var csvPrinter: CSVPrinter = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    writer= new BufferedWriter(new FileWriter(filePath, true))
    csvPrinter= new CSVPrinter(writer, CSVFormat.DEFAULT)
    writer!=null && csvPrinter!=null
  }

  override def process(saveInfo: SaveInfo_Java): Unit = {
    saveInfo.recordTimer()
    val list: util.ArrayList[lang.Long] = saveInfo.getTimerRecord

    try {
      if (new File(filePath).exists()) {
        csvPrinter.println() // 插入一个空白行
        csvPrinter.flush()
      }

      list.asScala.foreach { element =>
        csvPrinter.print(element.toString) // 逐个打印每个元素
      }

    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (writer!=null && csvPrinter!=null) {
      csvPrinter.close()
      writer.close()
    }
  }
}
