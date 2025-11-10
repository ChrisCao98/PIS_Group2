package MyUtil


import java.awt.{Frame, Graphics, Image, MediaTracker, Toolkit}
import java.awt.event.{WindowAdapter, WindowEvent}
import java.awt.image.BufferedImage
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, IOException}
import javax.imageio.ImageIO

/**
 * I don't use this GUI. This GUI is written by scala.
 */
object ImageGUI extends Frame {

  private var byteArray: Array[Byte] = _

  def setByteArray(byteArray: Array[Byte]): Unit = {
    this.byteArray = byteArray
  }

  def displayImage(): Unit = {
    try {
      val bis = new ByteArrayInputStream(byteArray)
      val bufferedImage = ImageIO.read(bis)
      val image: Image = Toolkit.getDefaultToolkit.createImage(bufferedImage.getSource)
      val mediaTracker = new MediaTracker(this)
      mediaTracker.addImage(image, 0)
      mediaTracker.waitForAll()

      val imageWidth = image.getWidth(null)
      val imageHeight = image.getHeight(null)

      setSize(1500, 1000)

      addWindowListener(new WindowAdapter() {
        override def windowClosing(we: WindowEvent): Unit = {
          dispose()
        }
      })

      setVisible(true)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  override def paint(g: Graphics): Unit = {
    val image: Image = Toolkit.getDefaultToolkit.createImage(byteArray)
    g.drawImage(image, 0, 0, this)
  }

  def main(args: Array[String]): Unit = {
    val byteArray: Array[Byte] = readImage("/home/chriscao/IdeaProjects/kfaka_no_gui/src/main/resources/testImage/image.jpg")
    ImageGUI.setByteArray(byteArray)
    ImageGUI.displayImage()
  }

  def readImage(filePath: String): Array[Byte] = {
    try {
      val imageFile = new File(filePath)
      val bufferedImage = ImageIO.read(imageFile)
      val outputStream = new ByteArrayOutputStream()
      ImageIO.write(bufferedImage, "jpg", outputStream)
      outputStream.toByteArray
    } catch {
      case e: IOException =>
        e.printStackTrace()
        Array.emptyByteArray
    }
  }
}
