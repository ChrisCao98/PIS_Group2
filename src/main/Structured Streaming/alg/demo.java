package alg;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class demo {
    public static void main(String[] args) throws Exception {
        // 创建SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("CSV Streaming Demo")
                .master("local[*]")
                .getOrCreate();

        // 读取CSV文件作为流式数据源
        StructType gpsSchema = new StructType(new StructField[]{
                new StructField("timestamp", DataTypes.DoubleType, true, null),
                new StructField("lat", DataTypes.DoubleType, true, null),
                new StructField("lon", DataTypes.DoubleType, true, null),
                new StructField("alt", DataTypes.DoubleType, true, null),
                new StructField("roll", DataTypes.DoubleType, true, null),
                new StructField("pitch", DataTypes.DoubleType, true, null),
                new StructField("yaw", DataTypes.DoubleType, true, null),
                new StructField("vn", DataTypes.DoubleType, true, null),
                new StructField("ve", DataTypes.DoubleType, true, null),
                new StructField("vf", DataTypes.DoubleType, true, null),
                new StructField("vl", DataTypes.DoubleType, true, null),
                new StructField("vu", DataTypes.DoubleType, true, null),
                new StructField("ax", DataTypes.DoubleType, true, null),
                new StructField("ay", DataTypes.DoubleType, true, null),
                new StructField("az", DataTypes.DoubleType, true, null),
                new StructField("af", DataTypes.DoubleType, true, null),
                new StructField("al", DataTypes.DoubleType, true, null),
                new StructField("au", DataTypes.DoubleType, true, null),
                new StructField("wx", DataTypes.DoubleType, true, null),
                new StructField("wy", DataTypes.DoubleType, true, null),
                new StructField("wz", DataTypes.DoubleType, true, null),
                new StructField("wf", DataTypes.DoubleType, true, null),
                new StructField("wl", DataTypes.DoubleType, true, null),
                new StructField("wu", DataTypes.DoubleType, true, null),
                new StructField("pos_accuracy", DataTypes.DoubleType, true, null),
                new StructField("vel_accuracy", DataTypes.DoubleType, true, null),
                new StructField("navstat", DataTypes.IntegerType, true, null),
                new StructField("numsats", DataTypes.IntegerType, true, null),
                new StructField("posmode", DataTypes.IntegerType, true, null),
                new StructField("velmode", DataTypes.IntegerType, true, null),
                new StructField("orimode", DataTypes.IntegerType, true, null),

        });
//                StructType GPSSCHEMA = new StructType(new StructField[]{
//                new StructField("timestamp", DataTypes.IntegerType, true, null),
//                new StructField("lat", DataTypes.IntegerType, true, null),
//                new StructField("alt", DataTypes.IntegerType, true, null)});
//        Dataset<Row> streamingData = spark.readStream()
//                .format("csv")
//                .schema(gpsSchema)
//                .option("header", false)
////                .option("inferSchema", true)
//                .load("/home/chriscao/IdeaProjects/data/csvdata/gps_info.csv");

        Dataset<Row> streamingData = spark
                .readStream()
                .option("delimiter", ",")
                .schema(gpsSchema)
                .option("header", false)
                .csv("/home/chriscao/IdeaProjects/data/csvdata");

        // 输出流式数据到控制台
        streamingData.writeStream()
                .format("console")
                .start()
                .awaitTermination();


    }
}