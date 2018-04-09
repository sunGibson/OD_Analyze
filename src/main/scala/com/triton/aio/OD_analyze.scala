package com.triton.aio

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by cuitu on 2018/3/26.
  */
object OD_analyze {
  def main(args: Array[String]) {
    val GSMFilePath = "hdfs://master:9000/user/yuty/data/"
    val GSMFileName = "GSM20140303MD.csv"
    val GSMStationName = "BasestationSArea2014.csv"

    //初始化sparkSession
    val spark = SparkSession.builder().master("spark://master:7077").appName("OD_Analyze").getOrCreate()
    //读取GSM数据
    val GSMDATA = spark.read.format("csv").option("header", true).option("inferSchema", false).load(GSMFilePath + GSMFileName)
    //时间格式转换
    val TS = unix_timestamp(GSMDATA("FDT_TIME"), "yyyy/MM/dd HH:mm:ss").cast("timestamp")
    GSMDATA.selectExpr("FSTR_MSID MSID", "CAST(FFLT_X AS FLOAT) X", "CAST(FFLT_Y AS FLOAT) Y", "FDT_TIME").withColumn("TIMESTAMP", TS).createOrReplaceTempView("GSMDATA_TABLE_0")
    //创建[MSID(string),X(float),Y(float),FDT_TIMR(string),TS(timestamp)]表,格式化GSM数据

    val GSMDATA_1 = spark.read.format("csv").option("header", true).option("inferSchema", false).load(GSMFilePath + GSMStationName)
    GSMDATA_1.selectExpr("CAST(X AS FLOAT) X", "CAST(Y AS FLOAT) Y", "Area").createOrReplaceTempView("GSMDATA_TABLE_1")
    //创建[X(float),Y(float),Area(string)]表，格式化小区数据

    spark.sql("SELECT A.MSID AS MSID,A.TIMESTAMP,A.X,A.Y,B.Area FROM GSMDATA_TABLE_0 A INNER JOIN GSMDATA_TABLE_1 B on A.X=B.X AND A.Y=B.Y ORDER BY MSID,TIMESTAMP").filter("TIMESTAMP is not null").createOrReplaceTempView("GSMDATA_TABLE_2")
    //创建[MSID(string),TIMESTAMP(timestamp),X(float),Y(float),Area(string)]表并过滤空值,匹配GSM数据到小区按照用户时间排序
    spark.sql("SELECT MSID,TIMESTAMP,MAX(X) X,MAX(Y) Y,MAX(Area) Area FROM GSMDATA_TABLE_2 GROUP BY MSID,TIMESTAMP").createOrReplaceTempView("GSMDATA_TABLE_3")
    //将GSMDATA_TABLE_2表中的重复时间数据过滤
    spark.sql("SELECT * FROM (SELECT *,row_number() over (PARTITION BY MSID,Area ORDER BY MSID,TIMESTAMP DESC) AS num FROM GSMDATA_TABLE_3) A WHERE A.num=1 UNION SELECT * FROM (SELECT *,row_number() over (PARTITION BY MSID,Area ORDER BY MSID,TIMESTAMP) AS num FROM GSMDATA_TABLE_3) B WHERE B.num=1").createOrReplaceTempView("GSMDATA_TABLE_4")
    //去除GSMDATA_TABLE_3表的中间点
    spark.sql("select MSID, TIMESTAMP, X, Y,Area from GSMDATA_TABLE_4 ORDER BY MSID,TIMESTAMP").createOrReplaceTempView("GSMDATA_TABLE_5")
    //对GSMDATA_TABLE_4表的数据按照MSID,TIMESTAMP进行排序
    spark.sql("SELECT MSID FROM GSMDATA_TABLE_4 GROUP BY MSID HAVING  count(MSID) > 1")createOrReplaceTempView("GSMDATA_TABLE_5")
    //挑选出符合的数据MSID（sparksql 不支持in，exsits等操作）
    spark.sql("SELECT MSID,TIMESTAMP,X,Y,Area FROM GSMDATA_TABLE_4 A LEFT SEMI JOIN GSMDATA_TABLE_5 B ON (A.MSID=B.MSID)").createOrReplaceTempView("GSMDATA_TABLE_6")
    //挑选出GSMDATA_TABLE_4中符合条件的数据
    spark.sql("SELECT MSID,X OX,Y OY,Area OA,TIMESTAMP ENTERTIME FROM GSMDATA_TABLE_6 ORDER BY MSID ,TIMESTAMP").withColumn("N", monotonically_increasing_id()).createOrReplaceTempView("GSMDATA_TABLE_7")
    //对GSMDATA_TABLE_6进行排序并添加N(自增)
    spark.sql("SELECT ENTERTIME DEPARTTIME,OX DX,OY DY,OA DA,N,MSID ID FROM GSMDATA_TABLE_7 WHERE N > 0").createOrReplaceTempView("GSMDATA_TABLE_8")
    spark.sql("SELECT A.MSID,A.OX,A.OY,A.OA,A.ENTERTIME,B.DX,B.DY,B.DA,B.DEPARTTIME,A.N,B.ID FROM GSMDATA_TABLE_7 A JOIN GSMDATA_TABLE_8 B ON A.N+1=B.N").createOrReplaceTempView("GSMDATA_TABLE_9")
    val ODDF = spark.sql("SELECT MSID,OX,OY,OA,ENTERTIME,DX,DY,DA,DEPARTTIME FROM GSMDATA_TABLE_9 WHERE MSID=ID")
    ODDF.createOrReplaceTempView("GSMDATA_TABLE_10")
    spark.sql("SELECT * FROM GSMDATA_TABLE_10 WHERE (cast(DEPARTTIME AS long)-cast(ENTERTIME AS long))>1800").createOrReplaceTempView("GSMDATA_TABLE_11")
    val OD_ANALYZE = spark.sql("SELECT OA,DA,count(*) ALLOD FROM GSMDATA_TABLE_11 GROUP BY OA,DA")
    OD_ANALYZE.write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save(GSMFilePath+"output/ALL_OD_ANALYZE")
    //最终的OD出行
    spark.close()

  }
}
