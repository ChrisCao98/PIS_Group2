package MyUtil

import org.apache.commons.csv.{CSVFormat, CSVPrinter}
import org.apache.spark.sql.ForeachWriter

import java.io.{BufferedWriter, File, FileWriter}
import java.{lang, util}
import scala.collection.JavaConverters.asScalaBufferConverter
/**
 * This class is a parent class that mainly inherits the ForeachWriter class and overrides the methods in it.
 */
class CommonWriter(filePath: String) extends ForeachWriter[SaveInfo_Java]{
//  var filePath: String = _
  var writer: BufferedWriter = _
  var csvPrinter: CSVPrinter = _
  /**
   * This function is mainly used to create an object(csvPrinter).
   */
  override def open(partitionId: Long, epochId: Long): Boolean = {
    writer= new BufferedWriter(new FileWriter(filePath, true))
    csvPrinter= new CSVPrinter(writer, CSVFormat.DEFAULT)
    writer!=null && csvPrinter!=null
  }
  /**
   * This function is mainly used to write data into .csv file.
   */
  override def process(saveInfo: SaveInfo_Java): Unit = {
    saveInfo.recordTimer()
    val list: util.ArrayList[lang.Long] = saveInfo.getTimerRecord

    try {
      if (new File(filePath).exists()) {
        csvPrinter.println()
        csvPrinter.flush()
      }

      list.asScala.foreach { element =>
        csvPrinter.print(element.toString)
      }

    }
  }
  /**
   * This function is used to close the connection to avoid taking up resources.
   */
  override def close(errorOrNull: Throwable): Unit = {
    if (writer!=null && csvPrinter!=null) {
      csvPrinter.close()
      writer.close()
    }
  }
}
