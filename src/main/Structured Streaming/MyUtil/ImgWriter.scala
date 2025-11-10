package MyUtil

import alg.StructuredStreaming.pathInfo
/**
 * This Sink is used for image data.And show it an the GUI.
 */
class ImgWriter extends CommonWriter("/home/chriscao/IdeaProjects/PIS_Group2/data/img.csv") {
  override def process(saveInfo: SaveInfo_Java): Unit = {
    super.process(saveInfo)

    val imageBytes = saveInfo.getImg
    if(imageBytes != null){
      pathInfo.getGUI_img.displayImage(imageBytes)
    }
  }

}

