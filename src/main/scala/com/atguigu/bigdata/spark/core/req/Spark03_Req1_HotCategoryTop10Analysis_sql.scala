package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Req1_HotCategoryTop10Analysis_sql {

    def main(args: Array[String]): Unit = {

        // TODO : Top10热门品类
        // todo 根据每个品类的点击，下单，支付的量统计热门品类top10
        // todo  1 split  2 lateral view explode   3 case when    4 group by order by
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        val sc = spark.sparkContext
        import spark.implicits._


        // 1. 读取原始日志数据
        val actionDF: DataFrame = sc.textFile("datas/user_visit_action.txt", 2)
          .map(
              line => {
                  val data = line.split("_")
                  (data(2), data(6),data(8), data(10))
              }
          ).toDF("sessionID", "clickCID", "orderCIDs", "payCIDs")
        val inputTable = "actionTable"
        actionDF.cache()
        actionDF.createTempView(inputTable)
        // 四种action 搜索，点击，下单，支付
        // todo 需求1： 根据点击 下单 支付的数量 找出最热门的top10 品类
        // todo t1 将orderCIDs, payCIDs炸裂侧写， 并过滤搜索的数据
        // todo t2 新建CID, clickOne, orderOne, payOne
        // todo 最后根据CID group by  根据聚合后的clickSum, orderSum, paySum order by limit10返回结果
        var sqlText =
            s"""
               |with t1 as (
               |select
               |    clickCID
               |    ,orderCID
               |    ,payCID
               |from ${inputTable}
               |lateral view explode(split(orderCIDs, ',')) tmp1 as orderCID
               |lateral view explode(split(payCIDs, ',')) tmp2 as payCID
               |where clickCID != '-1' or orderCID != 'null' or payCID != 'null'
               |)
               |,
               |t2 as (
               |select
               |    (case when clickCID != '-1' then clickCID
               |    when orderCID != 'null' then orderCID
               |    when payCID != 'null' then payCID
               |    else -1 end) as CID
               |    , (case when clickCID != '-1' then 1 else 0 end) as clickOne
               |    , (case when orderCID != 'null' then 1 else 0 end) as orderOne
               |    , (case when payCID != 'null' then 1 else 0 end) as payOne
               |from t1
               |)
               |
               |select
               |    CID
               |    ,sum(clickOne) clickSum
               |    ,sum(orderOne) orderSum
               |    ,sum(payOne) paySum
               |from t2
               |group by CID
               |order by clickSum desc, orderSum desc, paySum desc
               |limit 10
               |""".stripMargin
        // todo 可以改变DF分区数
        val resultDF: DataFrame = spark.sql(sqlText).repartition(1)
        resultDF.cache()
        val HotCateIdTable = "HotCateIdTable"
        resultDF.createTempView(HotCateIdTable)
        // todo 存在了 spark-warehouse目录下
//        resultDF.write.mode(SaveMode.Overwrite).saveAsTable("hot10Table")
        resultDF.write.mode(SaveMode.Overwrite).format("csv").save("outputs/hotTable_csv")
        /*
        +---+--------+--------+------+
        |CID|clickSum|orderSum|paySum|
        +---+--------+--------+------+
        | 15|    6120|    1672|  1259|
        |  2|    6119|    1767|  1196|
        | 20|    6098|    1776|  1244|
        | 12|    6095|    1740|  1218|
        | 11|    6093|    1781|  1202|
        | 17|    6079|    1752|  1231|
        |  7|    6074|    1796|  1252|
        |  9|    6045|    1736|  1230|
        | 19|    6044|    1722|  1158|
        | 13|    6036|    1781|  1161|
        +---+--------+--------+------+
         */

        // todo 需求2 根据已经找出的10个top10cateID, 每个CateId 找出它最热门的top10 sessionID  点击统计
        sqlText =
            s"""
               |select
               |    hot10CID
               |    ,concat_ws(',', collect_list(sessionID_cnt)) sessionID_cnt_list
               |from
               |(
               |    select
               |        hot10CID
               |        ,concat(sessionID, ':', cnt) sessionID_cnt
               |    from
               |    (
               |        select
               |            hot10CID
               |            ,sessionID
               |            ,cnt
               |            ,row_number() over(partition by hot10CID order by cnt desc) rk
               |        from
               |        (
               |            select
               |                hot10CID
               |                ,sessionID
               |                ,count(sessionID) cnt
               |            from
               |            (
               |                select
               |                    t1.clickCID hot10CID
               |                    ,t1.sessionID sessionID
               |                from
               |                (
               |                    select
               |                        clickCID
               |                        ,sessionID
               |                    from ${inputTable}
               |                    where clickCID != '-1'
               |                )t1
               |                join
               |                ${HotCateIdTable} t2
               |                on t1.clickCID = t2.CID
               |            ) t3
               |            group by hot10CID, sessionID
               |        ) t4
               |    ) t5
               |    where rk <= 10
               |) t6
               |group by hot10CID
               |""".stripMargin
        val resultSessionDF = spark.sql(sqlText).repartition(1)
        resultSessionDF.write.mode(SaveMode.Overwrite).format("csv").save("outputs/resultSession_csv")
        actionDF.unpersist()
        resultDF.unpersist()


        sc.stop()
    }
}
