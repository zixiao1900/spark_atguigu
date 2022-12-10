package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark06_SparkSQL_create_loadData {

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "root")

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

//        spark.sql("use atguigu")

        // todo 准备数据
        // 创建3张表 并插入数据
        spark.sql(
            """
              |CREATE TABLE IF NOT EXISTS `user_visit_action`(
              |  `date` string,
              |  `user_id` bigint,
              |  `session_id` string,
              |  `page_id` bigint,
              |  `action_time` string,
              |  `search_keyword` string,
              |  `click_category_id` bigint,
              |  `click_product_id` bigint,
              |  `order_category_ids` string,
              |  `order_product_ids` string,
              |  `pay_category_ids` string,
              |  `pay_product_ids` string,
              |  `city_id` bigint)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath 'datas/user_visit_action_sql.txt' overwrite into table user_visit_action
            """.stripMargin)

        spark.sql("select * from user_visit_action").show(5)
        /*
        +----------+-------+--------------------+-------+-------------------+--------------+-----------------+----------------+------------------+-----------------+----------------+---------------+-------+
        |      date|user_id|          session_id|page_id|        action_time|search_keyword|click_category_id|click_product_id|order_category_ids|order_product_ids|pay_category_ids|pay_product_ids|city_id|
        +----------+-------+--------------------+-------+-------------------+--------------+-----------------+----------------+------------------+-----------------+----------------+---------------+-------+
        |2019-07-17|     95|26070e87-1ad7-49a...|     37|2019-07-17 00:00:02|          手机|               -1|              -1|              null|             null|            null|           null|      3|
        |2019-07-17|     95|26070e87-1ad7-49a...|     48|2019-07-17 00:00:10|          null|               16|              98|              null|             null|            null|           null|     19|
        |2019-07-17|     95|26070e87-1ad7-49a...|      6|2019-07-17 00:00:17|          null|               19|              85|              null|             null|            null|           null|      7|
        |2019-07-17|     38|6502cdc9-cf95-4b0...|     29|2019-07-17 00:00:19|          null|               12|              36|              null|             null|            null|           null|      5|
        |2019-07-17|     38|6502cdc9-cf95-4b0...|     22|2019-07-17 00:00:28|          null|               -1|              -1|              null|             null|     15,1,20,6,4|       15,88,75|      9|
        +----------+-------+--------------------+-------+-------------------+--------------+-----------------+----------------+------------------+-----------------+----------------+---------------+-------+
         */

        spark.sql(
            """
              |CREATE TABLE IF NOT EXISTS `product_info`(
              |  `product_id` bigint,
              |  `product_name` string,
              |  `extend_info` string)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath 'datas/product_info.txt' overwrite into table product_info
            """.stripMargin)

        spark.sql("select * from product_info").show(5)
        /*
        +----------+------------+-----------+
        |product_id|product_name|extend_info|
        +----------+------------+-----------+
        |         1|      商品_1|       自营|
        |         2|      商品_2|       自营|
        |         3|      商品_3|       自营|
        |         4|      商品_4|       自营|
        |         5|      商品_5|       自营|
        +----------+------------+-----------+
         */

        spark.sql(
            """
              |CREATE TABLE IF NOT EXISTS `city_info`(
              |  `city_id` bigint,
              |  `city_name` string,
              |  `area` string)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(
            """
              |load data local inpath 'datas/city_info.txt' overwrite into table city_info
            """.stripMargin)

        spark.sql("""select * from city_info""").show(5)
        /*
        +-------+---------+----+
        |city_id|city_name|area|
        +-------+---------+----+
        |      1|     北京|华北|
        |      2|     上海|华东|
        |      3|     深圳|华南|
        |      4|     广州|华南|
        |      5|     武汉|华中|
        +-------+---------+----+
         */
        spark.sql("show tables").show() // 需要的3张表都在数据库中了
        /*
        +--------+-----------------+-----------+
        |database|        tableName|isTemporary|
        +--------+-----------------+-----------+
        | default|          atguigu|      false|
        | default|        city_info|      false|
        | default|     product_info|      false|
        | default|user_visit_action|      false|
        +--------+-----------------+-----------+
         */


        spark.close()
    }
}
