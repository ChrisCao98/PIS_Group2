package alg

import java.sql.Timestamp
import java.util

case class FinalGPSEvent(
                          timestamp: Timestamp,
                          location: List[(java.lang.Double, java.lang.Double)],
                          altitude: Double,
                          acc_x: Double,
                          acc_y: Double,
                          vel: Double,
//                          list : List[java.lang.Long]

                        )
