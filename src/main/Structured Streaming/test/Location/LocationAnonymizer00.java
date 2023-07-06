package test.Location;

import java.util.ArrayList;
import java.util.Arrays;
import scala.Tuple2;
import test.Speed.PETProcess;

public class LocationAnonymizer00 implements PETProcess<Tuple2<Double, Double>> {
    public LocationAnonymizer00() {
    }

    public ArrayList<Tuple2<Double, Double>> process(Tuple2<Double, Double> input) {
        return new ArrayList(Arrays.asList(input));
    }

    public static void main(String[] args) {
        LocationAnonymizer00 locAno = new LocationAnonymizer00();
        ArrayList<Tuple2<Double, Double>> process = locAno.process(new Tuple2(48.985771846331, 8.3941997039792));
        System.out.println(process);
    }
}
