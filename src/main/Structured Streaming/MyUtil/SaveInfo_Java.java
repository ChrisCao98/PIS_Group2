package MyUtil;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.io.Serializable;
/**
 * This class is used to extract important information and store it.
 */
public class SaveInfo_Java implements Serializable{
    private HashMap<String, Integer> PETPolicy;
    private Double timestamp;
    private ArrayList<Tuple2<Double, Double>> location;
    private Double altitude;
    private Double acc_x;
    private Double acc_y;
    private Double vel;

    private byte[] img;
    protected ArrayList<Long> TimerRecord;

    //record the time
    public void recordTimer(){
        TimerRecord.add(System.nanoTime());
    }

    public ArrayList<Long> getTimerRecord() {
        return TimerRecord;
    }
    //Constructor that initialises all id to 0
    public SaveInfo_Java() {
        PETPolicy = new HashMap<>();
        PETPolicy.put("SPEED", 0);
        PETPolicy.put("IMAGE", 0);
        PETPolicy.put("LOCATION", 0);
        TimerRecord = new ArrayList<>();
    }

    public SaveInfo_Java(Double timestamp, Double latitude, Double longitude, Double altitude, Double acc_x, Double acc_y, Double vel) {
        PETPolicy = new HashMap<>();
        PETPolicy.put("SPEED", 0);
        PETPolicy.put("IMAGE", 0);
        PETPolicy.put("LOCATION", 0);
        this.timestamp = timestamp;

        this.location = new ArrayList<>(Arrays.asList(new Tuple2<>(latitude, longitude)));
        this.altitude = altitude;
        this.acc_x = acc_x;
        this.acc_y = acc_y;
        this.vel = vel;
        TimerRecord = new ArrayList<>();
    }

    public HashMap<String, Integer> getPETPolicy() {
        return PETPolicy;
    }

    public void setPETPolicy(String key, Integer value) {
        this.PETPolicy.put(key, value);
    }

    public Tuple2<Double, Double> getPosition(){
        return location.get(0);
    }

    public ArrayList<Tuple2<Double, Double>> getLocation(){
        return location;
    }

    public void setLocation(ArrayList<Tuple2<Double, Double>> location) {
        this.location = location;
    }

    public double getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(double timestamp) {
        this.timestamp = timestamp;
    }

    public Double getLatitude() {
        return getPosition()._1;
    }

    public void setLatitude(Double latitude) {
        ArrayList<Tuple2<Double, Double>> updatedLocation = new ArrayList<>(Arrays.asList(new Tuple2<>(latitude, getLatitude())));
        location = updatedLocation;
    }

    public Double getLongitude() {
        return getPosition()._2;
    }

    public void setLongitude(Double longitude) {
        ArrayList<Tuple2<Double, Double>> updatedLocation = new ArrayList<>(Arrays.asList(new Tuple2<>(getLatitude(), longitude)));
        location = updatedLocation;
    }

    public Double getAltitude() {
        return altitude;
    }

    public void setAltitude(Double altitude) {
        this.altitude = altitude;
    }

    public Double getAcc_x() {
        return acc_x;
    }

    public void setAcc_x(Double acc_x) {
        this.acc_x = acc_x;
    }

    public Double getAcc_y() {
        return acc_y;
    }

    public void setAcc_y(Double acc_y) {
        this.acc_y = acc_y;
    }

    public Double getVel() {
        return vel;
    }

    public void setVel(Double vel) {
        this.vel = vel;
    }

    public byte[] getImg() {
        return img;
    }

    public void setImg(byte[] img) {
        this.img = img;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "PETPolicy=" + PETPolicy + "\n"+
                ", location=" + location +
                ", timestamp=" + timestamp +
                ", vel=" + vel +
                '}';
    }

    public static Double calculateVelocity(){

        return 0.0D;
    }
}
