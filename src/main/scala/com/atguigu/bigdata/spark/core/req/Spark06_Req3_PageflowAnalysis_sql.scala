package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Req3_PageflowAnalysis_sql {

  def main(args: Array[String]): Unit = {

    // TODO : 指定页面转化率 1-2  2-3  3-4
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val inputDF: DataFrame = sc.textFile("datas/user_visit_action.txt")
      .map(
        line => {
          val data = line.split("_")
          (data(2), data(3), data(4))
        }
      ).toDF("sessionId", "pageId", "actionTime")
    inputDF.cache()
    val inputTable = "inputTable"
    inputDF.createTempView(inputTable)

    val sqlText =
      s"""
         |with t1 as (
         |select
         |    pageId
         |    ,count(*) pageCnt
         |from ${inputTable}
         |where pageId in ('1','2','3','4','5','6')
         |group by pageId
         |)
         |,
         |
         |t2 as (
         |select
         |    max(pageIdIn) pageIdIn
         |    ,pageCombo
         |    ,count(*) pageComboCnt
         |from
         |(
         |    select
         |        pageIdIn
         |        ,pageCombo
         |        ,sessionId
         |        ,actionTime
         |    from
         |    (
         |        select
         |            pageIdIn
         |            ,concat(pageIdIn, '#', pageIdOut) pageCombo
         |            ,sessionId
         |            ,actionTime
         |        from
         |        (
         |            select
         |                pageId pageIdIn
         |                ,sessionId
         |                ,actionTime
         |                ,lead(pageId, 1) over(partition by sessionId order by actionTime) pageIdOut
         |            from ${inputTable}
         |        ) a1
         |    ) a2
         |    where pageCombo in ('1#2','2#3','3#4','4#5','5#6','6#7')
         |) a3
         |group by pageCombo
         |)
         |
         |select
         |    pageCombo
         |    ,t2.pageComboCnt / t1.pageCnt
         |from t1 join t2 on t1.pageId = t2.pageIdIn
         |""".stripMargin
    spark.sql(sqlText).show(false)
    inputDF.unpersist()
    /*
    t1: group by + count
    +------+-------+
    |pageId|pageCnt|
    +------+-------+
    |3     |3672   |
    |5     |3563   |
    |6     |3593   |
    |1     |3640   |
    |4     |3602   |
    |2     |3559   |
    +------+-------+
    t2: lead(pageId, 1) over(partition by sessionId order by actionTime)
    +--------+---------+------------+
    |pageIdIn|pageCombo|pageComboCnt|
    +--------+---------+------------+
    |4       |4#5      |66          |
    |1       |1#2      |55          |
    |3       |3#4      |62          |
    |5       |5#6      |52          |
    |6       |6#7      |69          |
    |2       |2#3      |71          |
    +--------+---------+------------+
    res:
    +---------+--------------------------------------------------------+
    |pageCombo|(CAST(pageComboCnt AS DOUBLE) / CAST(pageCnt AS DOUBLE))|
    +---------+--------------------------------------------------------+
    |3#4      |0.016884531590413945                                    |
    |5#6      |0.014594442885209093                                    |
    |6#7      |0.0192040077929307                                      |
    |1#2      |0.01510989010989011                                     |
    |4#5      |0.018323153803442533                                    |
    |2#3      |0.019949423995504357                                    |
    +---------+--------------------------------------------------------+
     */


    sc.stop()
  }
}
