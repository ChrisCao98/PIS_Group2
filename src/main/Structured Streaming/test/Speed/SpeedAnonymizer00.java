package test.Speed;



import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class SpeedAnonymizer00 implements PETProcess<Double>, Serializable {

    public SpeedAnonymizer00() {
    }

    public ArrayList<Double> process(Double input) {
        return new ArrayList(Arrays.asList(0.0));
    }

    public static void main(String[] args) {
        SpeedAnonymizer00 speedAnonymizer = new SpeedAnonymizer00();
        ArrayList<Double> process = speedAnonymizer.process(33.0);
        System.out.println(process.get(0));
    }
}
