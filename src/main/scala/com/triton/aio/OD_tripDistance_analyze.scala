package com.triton.aio

import org.apache.spark.sql.SparkSession

/**
  * Created by cuitu on 2018/4/17.
  */
object OD_tripDistance_analyze {
  def main(args: Array[String]): Unit = {
    var GSMFilePath = ""
    var GSMStationName = ""
    var OD_TRIP_ANALYZE_FILE = ""
    if(args.length == 2){
      GSMFilePath = args(0)
      GSMStationName = args(1)
      OD_TRIP_ANALYZE_FILE = GSMFilePath + "output/OD_TRIP_ANALYZE"
    }else{
      println("Please right input GSMFilePath、GSMStationName")
      System.exit(1)
    }

    val spark = SparkSession.builder().master("spark://master:7077").appName("OD_tripDistance_analyze").getOrCreate()
    spark.read.format("csv").option("header","true").option("inferSchema","true").load(GSMFilePath + GSMStationName).createOrReplaceTempView("TB1")
    spark.sql("SELECT Area, avg(X) X, avg(Y) Y FROM TB1 GROUP BY Area").createOrReplaceTempView("TB2")
    spark.read.format("csv").option("header","true").option("inferSchema","true").load(GSMFilePath + OD_TRIP_ANALYZE_FILE).createOrReplaceTempView("TB3")
    spark.sql("SELECT A.*, B.X OX, B.Y OY FROM TB3 A JOIN TB2 B ON A.OA = B.Area").createOrReplaceTempView("TB4")
    spark.sql("SELECT A.*, B.X DX, B.Y DY FROM TB4 A JOIN TB2 B ON A.DA = B.Area").createOrReplaceTempView("TB5")
    val OD_TRIPDISTANCE_ANALYZE = spark.sql("SELECT OA, DA, ALLOD, sqrt(power(DX-OX,2)+power(DY-OY,2))*111 DISTANCE FROM TB5")

    OD_TRIPDISTANCE_ANALYZE.write.format("csv").option("header","true").save(GSMFilePath + "output/OD_TRIPDISTANCE_ANALYZE")
    spark.close()
  }
}
