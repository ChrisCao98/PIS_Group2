package Util


import scala.math.sqrt

object MathUtil_Scala {
  def calculateDistance(p1: (Double, Double), p2: (Double, Double)): Double = {
    val x1 = p1._1
    val y1 = p1._2
    val x2 = p2._1
    val y2 = p2._2
    sqrt((y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1))
  }

}
