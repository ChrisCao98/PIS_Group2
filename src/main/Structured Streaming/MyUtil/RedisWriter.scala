package MyUtil

import org.apache.spark.sql.ForeachWriter
import redis.clients.jedis.Jedis
/**
 *  The purpose of this class is to accept input from the user and
 *  then write to the redis server based on the user's input.
 */
class RedisWriter extends ForeachWriter[String] {
  private var jedisClient: Jedis = _
  //Open the connection to get an instance
  override def open(partitionId: Long, epochId: Long): Boolean = {
    jedisClient = RedisUtil.getJedisClient
    jedisClient != null && jedisClient.isConnected
  }
  //Determine what action to do based on the input.
  override def process(value: String): Unit = {
    if (value.startsWith("change")) {
      // change the user-specified PET policy
      val fields = value.split(",")
      jedisClient.set("SpeedPET", fields(3))
      jedisClient.set("LocationPET", fields(1))
      jedisClient.set("CameraPET", fields(2))

      System.out.println("Change policy setting!")
    }
    else if (value.startsWith("situation")) {
      val fields = value.split(",")
      fields(1) match {
        case "speed" =>
          var SpeedSituation = if (jedisClient.get("SpeedSituation") == "0") false else true
//          var SpeedSituation = jedisClient.get("SpeedSituation").toBoolean
          SpeedSituation = !SpeedSituation
          val numericValue: Int = if (SpeedSituation) 1 else 0
          jedisClient.set("SpeedSituation", numericValue.toString)
          System.out.println("Switch Speed environment!" + SpeedSituation)

        case "camera" =>

          var CameraSituation = if (jedisClient.get("CameraSituation") == "0") false else true
          CameraSituation = !CameraSituation
          val numericValue: Int = if (CameraSituation) 1 else 0
          jedisClient.set("CameraSituation", numericValue.toString)
          System.out.println("Switch camera environment!" + CameraSituation)

        case _ =>
          System.out.println("Not Valid 'situation' input!")
      }
    }
    else System.out.println("Not valid 'user config input!")



//    val words = value.split("\\W+")
//    val key = words(0)
//    val vakue = words(1)
//    key match {
//      case "SPEED" =>
//        jedisClient.set(key, vakue)
//      case "LOCATION" =>
//        jedisClient.set(key, vakue)
//      case "IMAGE" =>
//        jedisClient.set(key, vakue)
//      case _ =>
//        println("Wrong command.")
//    }
  }
  //close the connection
  override def close(errorOrNull: Throwable): Unit = {
    if (jedisClient != null && jedisClient.isConnected) {
      jedisClient.close()
    }
  }
}

