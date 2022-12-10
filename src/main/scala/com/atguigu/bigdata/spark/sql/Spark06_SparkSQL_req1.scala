package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark06_SparkSQL_req1 {

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "root")

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
        // todo 1 求各个区域热门商品top3 热门是从点击量的维度看
        // todo 2 并备注每个商品在主要城市的分布比例 超过两个用其他显示
        /*
        地区  商品名称    次数  备注
        华北  商品A      1000  北京20%，天津13%，其他65%
        华北  商品B      8200  北京63%，太原10%，其他27%
        华北  商品M      2800  北京25%，太原10%，其他65%
        。。。
         */
//        spark.sql("use atguigu")
        // todo 这里做需求1  3张表关联 连续join
        spark.sql(
            """
              |select
              |    area,
              |    product_name,
              |    clickCnt,
              |    rank
              |from (
              |    select
              |        area,
              |        product_name,
              |        clickCnt,
              |        rank() over( partition by area order by clickCnt desc ) as rank
              |    from (
              |        select
              |           area,
              |           product_name,
              |           count(*) as clickCnt
              |        from (
              |            select
              |               a.*,
              |               p.product_name,
              |               c.area,
              |               c.city_name
              |            from user_visit_action a
              |            join product_info p
              |            on a.click_product_id = p.product_id
              |            join city_info c
              |            on a.city_id = c.city_id
              |            where a.click_product_id > -1
              |        ) t1 group by area, product_name
              |    ) t2
              |) t3 where rank <= 3
            """.stripMargin).show

        /*
        +----+------------+--------+----+
        |area|product_name|clickCnt|rank|
        +----+------------+--------+----+
        |华东|     商品_86|     371|   1|
        |华东|     商品_47|     366|   2|
        |华东|     商品_75|     366|   2|
        |西北|     商品_15|     116|   1|
        |西北|      商品_2|     114|   2|
        |西北|     商品_22|     113|   3|
        |华南|     商品_23|     224|   1|
        |华南|     商品_65|     222|   2|
        |华南|     商品_50|     212|   3|
        |华北|     商品_42|     264|   1|
        |华北|     商品_99|     264|   1|
        |华北|     商品_19|     260|   3|
        |东北|     商品_41|     169|   1|
        |东北|     商品_91|     165|   2|
        |东北|     商品_58|     159|   3|
        |东北|     商品_93|     159|   3|
        |华中|     商品_62|     117|   1|
        |华中|      商品_4|     113|   2|
        |华中|     商品_57|     111|   3|
        |华中|     商品_29|     111|   3|
        +----+------------+--------+----+
         */


        spark.close()
    }
}
