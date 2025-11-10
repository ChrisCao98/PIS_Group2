package MyUtil


import scala.collection.mutable
import scala.collection.mutable.HashMap

class SaveInfo_Scala(
                      var PETPolicy: mutable.HashMap[String, Int] = mutable.HashMap("SPEED" -> 0, "IMAGE" -> 0, "LOCATION" -> 0),
                      var timestamp: Double = 0.0,
                      var location: List[(Double, Double)] = List.empty,
                      var altitude: Double = 0.0,
                      var acc_x: Double = 0.0,
                      var acc_y: Double = 0.0,
                      var vel: Double = 0.0,
                      var img: Array[Byte] = Array.emptyByteArray
                   ) extends Serializable {

  def getPosition: (Double, Double) = location.headOption.getOrElse((0.0, 0.0))

  def setLatitude(latitude: Double): Unit = {
    location = List((latitude, getPosition._2))
  }

  def setLongitude(longitude: Double): Unit = {
    location = List((getPosition._1, longitude))
  }

  def setPETPolicy(key: String, value: Int): Unit = {
    PETPolicy(key) = value
  }

}

object SaveInfo {
  def apply(timestamp: Double, latitude: Double, longitude: Double, altitude: Double, acc_x: Double, acc_y: Double, vel: Double): SaveInfo_Scala = {
    val petPolicy = HashMap("SPEED" -> 0, "IMAGE" -> 0, "LOCATION" -> 0)
    val location = List((latitude, longitude))
    val img = Array.emptyByteArray
    new SaveInfo_Scala(petPolicy, timestamp, location, altitude, acc_x, acc_y, vel, img)
  }

  def apply(): SaveInfo_Scala = {
    new SaveInfo_Scala()
  }


}

