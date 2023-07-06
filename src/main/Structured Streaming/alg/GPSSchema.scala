package alg

import org.apache.spark.sql.types._

class GPSSchema {
  val schema: StructType = StructType(Seq(
    StructField("timestamp", DataTypes.DoubleType, nullable = true),
    StructField("lat", DataTypes.DoubleType, nullable = true),
    StructField("lon", DataTypes.DoubleType, nullable = true),
    StructField("alt", DataTypes.DoubleType, nullable = true),
    StructField("roll", DataTypes.DoubleType, nullable = true),
    StructField("pitch", DataTypes.DoubleType, nullable = true),
    StructField("yaw", DataTypes.DoubleType, nullable = true),
    StructField("vn", DataTypes.DoubleType, nullable = true),
    StructField("ve", DataTypes.DoubleType, nullable = true),
    StructField("vf", DataTypes.DoubleType, nullable = true),
    StructField("vl", DataTypes.DoubleType, nullable = true),
    StructField("vu", DataTypes.DoubleType, nullable = true),
    StructField("ax", DataTypes.DoubleType, nullable = true),
    StructField("ay", DataTypes.DoubleType, nullable = true),
    StructField("az", DataTypes.DoubleType, nullable = true),
    StructField("af", DataTypes.DoubleType, nullable = true),
    StructField("al", DataTypes.DoubleType, nullable = true),
    StructField("au", DataTypes.DoubleType, nullable = true),
    StructField("wx", DataTypes.DoubleType, nullable = true),
    StructField("wy", DataTypes.DoubleType, nullable = true),
    StructField("wz", DataTypes.DoubleType, nullable = true),
    StructField("wf", DataTypes.DoubleType, nullable = true),
    StructField("wl", DataTypes.DoubleType, nullable = true),
    StructField("wu", DataTypes.DoubleType, nullable = true),
    StructField("pos_accuracy", DataTypes.DoubleType, nullable = true),
    StructField("vel_accuracy", DataTypes.DoubleType, nullable = true),
    StructField("navstat", DataTypes.IntegerType, nullable = true),
    StructField("numsats", DataTypes.IntegerType, nullable = true),
    StructField("posmode", DataTypes.IntegerType, nullable = true),
    StructField("velmode", DataTypes.IntegerType, nullable = true),
    StructField("orimode", DataTypes.IntegerType, nullable = true)
  ))
}

