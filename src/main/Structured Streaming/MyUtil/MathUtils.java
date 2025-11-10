package MyUtil;

import scala.Tuple2;

import static java.lang.Math.sqrt;
/**
 * This class is used to calculate the distance between two points.
 */
public class MathUtils {
    public static Double calculateDistance(Tuple2<Double, Double> p1, Tuple2<Double, Double> p2){
        Double x1 = p1._1;
        Double y1 = p1._2;
        Double x2 = p2._1;
        Double y2 = p2._2;
        return sqrt((y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1));
    }
}
