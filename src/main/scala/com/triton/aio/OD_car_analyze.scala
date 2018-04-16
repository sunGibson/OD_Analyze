package com.triton.aio

import org.apache.spark.sql._

/**
  * Created by cuitu on 2018/4/10.
  */
object OD_car_analyze {
  def main(args: Array[String]): Unit = {
    var CAR_GPS_FilePath = "hdfs://master:9000/user/yuty/data/"
    var CAR_GPS_FileName = "all_gps_1603"
    if ( args.length == 2 ){
      CAR_GPS_FilePath = args(0)
      CAR_GPS_FileName = args(1)
    }else{
      println("Please right input CAR_GPS_FilePath、CAR_GPS_FileName")
      System.exit(1)
    }

    val spark = SparkSession.builder().master("spark://master:7077").appName("OD_car_analyze").getOrCreate()
    val ORIGINAL_DATA = spark.read.format("csv").option("delimiter","|").load(CAR_GPS_FilePath+CAR_GPS_FileName)
    //读取数据
    val GPS_DATA = ORIGINAL_DATA.selectExpr("_c0 CARID","CAST(_c3 AS INT) EMPTY","CAST(_c9 as timestamp) TIME","CAST(_c10 as DOUBLE) LONGITUDE","CAST(_c11 AS DOUBLE) LATITUDE","CAST(_c12 AS DOUBLE) SPEED")
    GPS_DATA.filter(GPS_DATA("EMPTY") === 0 || GPS_DATA("EMPTY") === 1).createOrReplaceTempView("TB1")
    //过滤无效数据
    spark.sql("SELECT *,row_number() over(PARTITION BY CARID ORDER BY CARID,TIME) N FROM TB1").createOrReplaceTempView("TB2")
    spark.sql("SELECT CARID MIDID,EMPTY MID_EMPTY,N M FROM TB2 WHERE N > 0").createOrReplaceTempView("TB3")
    val OD_CAR_DATA = spark.sql("SELECT A.*,B.* FROM TB2 A JOIN TB3 B ON  A.N+1 = B.M AND A.CARID = B.MIDID AND EMPTY <> MID_EMPTY ORDER BY CARID,TIME")
    //获取OD点数据
    OD_CAR_DATA.write.mode(SaveMode.Overwrite).format("csv").option("header","true").save(CAR_GPS_FilePath + "output/OD_CAR_DATA")

  }


}
