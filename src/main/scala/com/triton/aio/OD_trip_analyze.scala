package main.scala.com.triton.aio

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by cuitu on 2018/4/9.
  */
object OD_department_analyze {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("spark://master:7077").appName("OD_Analyze").getOrCreate()
    //读取GSM数据，并转换数据格式
    val GSM20140303MD = spark.read.format("csv").option("header", "true").option("inferSchema", "false").load("hdfs://master:9000/user/yuty/data/GSM20140303MD0.csv")
    //做数据转换

    val ts = unix_timestamp(GSM20140303MD("FDT_TIME"), "yyyy/MM/dd HH:mm:ss").cast("timestamp")
    val GSM20140303MDa = GSM20140303MD.selectExpr("FSTR_MSID MSID", "cast(FFLT_X as float) X", "cast(FFLT_Y as float) Y", "FDT_TIME").withColumn("TIMESTAMP", ts)
    GSM20140303MDa.createOrReplaceTempView("GSM20140303MDaTable")
    //读取研究区域基站数据
    val basestationA = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://master:9000/user/yuty/data/BasestationSArea2014.csv")
    val basestationA1 = basestationA.selectExpr("cast(X as float) X", "cast(Y as float) Y", " Area")
    basestationA1.createOrReplaceTempView("basestationATable")

    //计算用户夜间居住地
    val GSM20140303MDb = spark.sql("select * from GSM20140303MDaTable where TIMESTAMP between '2014-03-03 00:00:00' and '2014-03-03 06:00:00' ")
    GSM20140303MDb.createOrReplaceTempView("GSM20140303MDbTable")

    //根据用户ID、经纬度分组统计
    val GSM20140303MDc = spark.sql("select MSID,X,Y,count(*) N from GSM20140303MDbTable group by MSID,X,Y")
    GSM20140303MDc.createOrReplaceTempView("GSM20140303MDcTable")

    //对数据进行分组、排序统计
    val GSM20140303MDd1 = spark.sql("select *,row_number() over (PARTITION BY MSID order by N Desc) as ID from GSM20140303MDcTable")
    GSM20140303MDd1.createOrReplaceTempView("GSM20140303MDd1Table")

    //获取每个用户占比最多经纬度的数据
    val GSM20140303MDd2 = spark.sql("select MSID,X,Y,N from GSM20140303MDd1Table where ID=1")
    GSM20140303MDd2.createOrReplaceTempView("GSM20140303MDdTable")

    //筛选研究区域的有效用户、过滤不能与区域经纬度匹配的点
    val GSM20140303MDe = spark.sql("select A.MSID as MSID,A.X,A.Y,B.Area FROM GSM20140303MDdTable A inner join basestationATable B on A.X=B.X AND A.Y=B.Y")
    GSM20140303MDe.createOrReplaceTempView("GSM20140303MDeTable")

    //0)输出用户居住地数据
    GSM20140303MDe.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header", "true").save("/user/sunjp/data/User20140303AllS.csv")
    GSM20140303MDe.show(false) // coalesce(1).   emerged all file to one
    spark.stop()
  }
}
