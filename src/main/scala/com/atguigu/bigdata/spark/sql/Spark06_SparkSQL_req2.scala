package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Spark06_SparkSQL_req2 {

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

        // 查询基本数据
        spark.sql(
            """
              |  select
              |     p.product_name,
              |     c.area,
              |     c.city_name
              |  from user_visit_action a
              |  join product_info p on a.click_product_id = p.product_id
              |  join city_info c on a.city_id = c.city_id
              |  where a.click_product_id > -1
            """.stripMargin).createOrReplaceTempView("t1")
        spark.sql("select * from t1").show(10)
        /*
        +------------+----+---------+
        |product_name|area|city_name|
        +------------+----+---------+
        |     商品_98|华北|     保定|
        |     商品_85|华北|     天津|
        |     商品_36|华中|     武汉|
        |     商品_44|华南|     广州|
        |     商品_79|华东|     上海|
        |     商品_50|华南|     厦门|
        |     商品_39|东北|     大连|
        |     商品_62|华南|     福州|
        |     商品_58|东北|   哈尔滨|
        |     商品_68|华东|     无锡|
        +------------+----+---------+
         */

        // 根据区域，商品进行数据聚合
        spark.udf.register("cityRemark", functions.udaf(new CityRemarkUDAF()))
        spark.sql(
            """
              |  select
              |     area,
              |     product_name,
              |     count(*) as clickCnt,
              |     cityRemark(city_name) as city_remark
              |  from t1 group by area, product_name
            """.stripMargin).createOrReplaceTempView("t2")
        spark.sql("select * from t2").show(10, false)
        /*
        +----+------------+--------+---------------------------------+
        |area|product_name|clickCnt|city_remark                      |
        +----+------------+--------+---------------------------------+
        |华东|商品_53     |345     |杭州 17.1, 上海 14.2, 其他 68.7  |
        |华东|商品_72     |311     |青岛 18.6, 上海 16.4, 其他 65.0  |
        |华中|商品_69     |110     |长沙 55.5, 武汉 44.5             |
        |华北|商品_94     |235     |天津 21.3, 郑州 20.9, 其他 57.9  |
        |东北|商品_100    |131     |哈尔滨 35.9, 沈阳 33.6, 其他 30.5|
        |华东|商品_58     |350     |济南 18.6, 上海 15.7, 其他 65.7  |
        |西北|商品_43     |98      |银川 52.0, 西安 48.0             |
        |西北|商品_59     |92      |西安 63.0, 银川 37.0             |
        |西南|商品_9      |138     |贵阳 42.0, 成都 30.4, 其他 27.5  |
        |华南|商品_11     |209     |厦门 29.7, 深圳 29.7, 其他 40.7  |
        +----+------------+--------+---------------------------------+

         */

        // 区域内对点击数量进行排行
        spark.sql(
            """
              |  select
              |      area,
              |      product_name,
              |      clickCnt,
              |      city_remark,
              |      rank
              |  from
              |  (
              |      select
              |          area,
              |          product_name,
              |          clickCnt,
              |          city_remark,
              |          rank() over( partition by area order by clickCnt desc ) as rank
              |      from t2
              |  ) t3
              |  where rank <= 3
            """.stripMargin).show(false)

        /*
        +----+------------+--------+---------------------------------+----+
        |area|product_name|clickCnt|city_remark                      |rank|
        +----+------------+--------+---------------------------------+----+
        |华东|商品_86     |371     |上海 16.4, 杭州 15.9, 其他 67.7  |1   |
        |华东|商品_47     |366     |杭州 15.8, 青岛 15.6, 其他 68.6  |2   |
        |华东|商品_75     |366     |上海 17.5, 无锡 15.6, 其他 66.9  |2   |
        |西北|商品_15     |116     |西安 54.3, 银川 45.7             |1   |
        |西北|商品_2      |114     |银川 53.5, 西安 46.5             |2   |
        |西北|商品_22     |113     |西安 54.9, 银川 45.1             |3   |
        |华南|商品_23     |224     |厦门 29.0, 福州 24.6, 其他 46.4  |1   |
        |华南|商品_65     |222     |深圳 27.9, 厦门 26.6, 其他 45.5  |2   |
        |华南|商品_50     |212     |福州 27.4, 深圳 25.9, 其他 46.7  |3   |
        |华北|商品_42     |264     |郑州 25.0, 保定 25.0, 其他 50.0  |1   |
        |华北|商品_99     |264     |北京 24.2, 郑州 23.5, 其他 52.3  |1   |
        |华北|商品_19     |260     |郑州 23.5, 保定 20.4, 其他 56.2  |3   |
        |东北|商品_41     |169     |哈尔滨 35.5, 大连 34.9, 其他 29.6|1   |
        |东北|商品_91     |165     |哈尔滨 35.8, 大连 32.7, 其他 31.5|2   |
        |东北|商品_58     |159     |沈阳 37.7, 大连 32.1, 其他 30.2  |3   |
        |东北|商品_93     |159     |哈尔滨 38.4, 大连 37.1, 其他 24.5|3   |
        |华中|商品_62     |117     |武汉 51.3, 长沙 48.7             |1   |
        |华中|商品_4      |113     |长沙 53.1, 武汉 46.9             |2   |
        |华中|商品_57     |111     |武汉 55.0, 长沙 45.0             |3   |
        |华中|商品_29     |111     |武汉 50.5, 长沙 49.5             |3   |
        +----+------------+--------+---------------------------------+----+


Process finished with exit code 0

         */

        spark.close()
    }
    case class Buffer( var total : Long, var cityMap:mutable.Map[String, Long] )
    // 自定义聚合函数：实现城市备注功能
    // 1. 继承Aggregator, 定义泛型
    //    IN ： 城市名称
    //    BUF : Buffer =>【总点击数量，Map[（city, cnt）, (city, cnt)]】
    //    OUT : 备注信息
    // 2. 重写方法（6）
    class CityRemarkUDAF extends Aggregator[String, Buffer, String]{
        // 缓冲区初始化
        override def zero: Buffer = {
            Buffer(0, mutable.Map[String, Long]())
        }

        // 更新缓冲区数据
        override def reduce(buff: Buffer, city: String): Buffer = {
            buff.total += 1
            val newCount = buff.cityMap.getOrElse(city, 0L) + 1
            buff.cityMap.update(city, newCount)
            buff
        }

        // 合并缓冲区数据
        override def merge(buff1: Buffer, buff2: Buffer): Buffer = {
            buff1.total += buff2.total

            val map1 = buff1.cityMap
            val map2 = buff2.cityMap

            // 两个Map的合并操作
//            buff1.cityMap = map1.foldLeft(map2) {
//                case ( map, (city, cnt) ) => {
//                    val newCount = map.getOrElse(city, 0L) + cnt
//                    map.update(city, newCount)
//                    map
//                }
//            }
            map2.foreach{
                case (city , cnt) => {
                    val newCount = map1.getOrElse(city, 0L) + cnt
                    map1.update(city, newCount)
                }
            }
            buff1.cityMap = map1
            buff1
        }
        // 将统计的结果生成字符串信息
        override def finish(buff: Buffer): String = {
            val remarkList: ListBuffer[String] = ListBuffer[String]()

            val totalcnt: Double = buff.total.toDouble
            val cityMap: mutable.Map[String, Long] = buff.cityMap

            // 降序排列
            val cityCntList = cityMap.toList.sortWith(
                (left, right) => {
                    left._2 > right._2
                }
            ).take(2)

            val hasMore = cityMap.size > 2
            var rsum: Double = 0.0
            cityCntList.foreach{
                case ( city, cnt ) => {
                    val r: Double = cnt * 100 / totalcnt
                    // todo 保留1位小数
                    remarkList.append(f"${city} ${r}%.1f")
                    rsum += r
                }
            }
            if ( hasMore ) {
                remarkList.append(f"其他 ${100 - rsum}%.1f")
            }

            remarkList.mkString(", ")
        }
        // 固定写法
        override def bufferEncoder: Encoder[Buffer] = Encoders.product
        // 固定写法
        override def outputEncoder: Encoder[String] = Encoders.STRING
    }
}
