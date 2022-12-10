package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Req_sql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req_sql")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val colNames: Array[String] = "ts,province,city,user,ad".split(",") // 字段一次性处理
    val tableName = "inputTable"
    sc.textFile("datas/agent.log")
      .map(line=>{
        val data = line.split("\\s+")
        (data(0), data(1), data(2), data(3), data(4))
      }).toDF(colNames:_*) // todo 这样字段就可以传字符串进来处理了
      .createTempView(tableName)
    // todo 完成功能 row_number() over()
    var sqltext =
      s"""
         |select
         |    province
         |    ,ad
         |    ,sum_
         |    ,rk
         |from
         |(
         |    select
         |        province
         |        ,ad
         |        ,sum_
         |        ,row_number() over(partition by province order by sum_ desc) rk
         |    from
         |    (
         |        select
         |            province
         |            ,ad
         |            ,sum(1) as sum_
         |            from
         |        ${tableName}
         |        group by province, ad
         |    )t1
         |)t2
         |where rk <= 3
         |""".stripMargin
    spark.sql(sqltext).createTempView("middleTable")
    // todo 展示 concat  concat_ws  collect_list
    sqltext =
      s"""
         |select
         |    province
         |    ,concat_ws('|', collect_list(ad_sum)) ad_top3
         |from
         |(
         |    select
         |        province
         |        ,concat(ad, ',', sum_) ad_sum
         |    from middleTable
         |) t1
         |group by province
         |""".stripMargin
    spark.sql(sqltext).show(100, false)
    /*
    +--------+-----------------+
    |province|ad_top3     |
    +--------+-----------------+
    |7       |16,26|26,25|1,23 |
    |3       |14,28|28,27|1,25 |
    |8       |2,27|20,23|11,22 |
    |0       |2,29|24,25|26,24 |
    |5       |14,26|12,21|21,21|
    |6       |16,23|24,21|22,20|
    |9       |1,31|28,21|0,20  |
    |1       |3,25|6,23|5,22   |
    |4       |12,25|2,22|16,22 |
    |2       |6,24|21,23|29,20 |
    +--------+-----------------+
     */

  }

}
