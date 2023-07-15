package alg

import Util.GUI
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser

import java.io.FileReader
import java.util
//here we can reach some pathInfo and GUI
case class PathInfo(confPath: String) {
  protected var PETconfpath: String = _
  protected var ImageOutputPath: String = _
  protected var PETType: util.ArrayList[String] = _
  private var GUI_img: GUI = _
  loadConfig()

  /**
   * initialization, read the configurations
   *
   * @param "confPath" The path of the file "Pipeconfig.json"
   */


  def loadConfig(): Unit = {
    val parser: JSONParser = new JSONParser()
    val obj: Any = parser.parse(new FileReader(confPath))
    val jsonObject: JSONObject = obj.asInstanceOf[JSONObject]
    PETconfpath = jsonObject.get("PET-CONF").asInstanceOf[String]
    PETType = jsonObject.get("PET-TYPE").asInstanceOf[util.ArrayList[String]]
    ImageOutputPath = jsonObject.get("IMAGE-OUTPUT-PATH").asInstanceOf[String]
    GUI_img = new GUI()
  }

  def loadPET(): Unit = {
    // TODO: implementation
    println("loadPET")
  }

  def getPETconfpath: String = PETconfpath

  def getImageOutputPath: String = PETconfpath

  def getPETType: util.ArrayList[String] = PETType

  def getGUI_img: GUI = GUI_img
}
