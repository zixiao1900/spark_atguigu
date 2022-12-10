package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Req1_HotCategoryTop10Analysis_flatMap {

    def main(args: Array[String]): Unit = {

        // TODO : Top10热门品类
        val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparConf)

        // Q : 存在大量的shuffle操作（reduceByKey）
        // reduceByKey 聚合算子，spark会提供优化，缓存

        // 1. 读取原始日志数据
        val actionRDD = sc.textFile("datas/user_visit_action.txt")

        // 2. todo 将数据转换结构 后期减少reduceByKey 只有一个
        //    点击的场合 : ( 品类ID，( 1, 0, 0 ) )
        //    下单的场合 : ( 品类ID，( 0, 1, 0 ) )
        //    支付的场合 : ( 品类ID，( 0, 0, 1 ) )
        val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
            action => {
                val datas = action.split("_")
                if (datas(6) != "-1") {
                    // 点击的场合 一条数据 对应一行
                    List((datas(6), (1, 0, 0)))
                }
                else if (datas(8) != "null") {
                    // 下单的场合
                    val ids: Array[String] = datas(8).split(",")
                    // 一条数据可能对应多行
                    ids.map(id => (id, (0, 1, 0)))
                }
                else if (datas(10) != "null") {
                    // 支付的场合
                    val ids = datas(10).split(",")
                    // 一条数据可能对应多行
                    ids.map(id => (id, (0, 0, 1)))
                }
                else {
                    Nil
                }
            }
        )

        // 3. 将相同的品类ID的数据进行分组聚合
        //    ( 品类ID，( 点击数量, 下单数量, 支付数量 ) )
        val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey(
            (t1, t2) => {
                ( t1._1+t2._1, t1._2 + t2._2, t1._3 + t2._3 )
            }
        )

        // 4. 将统计结果根据数量进行降序处理，取前10名
        val resultRDD = analysisRDD.sortBy(_._2, false).take(10)

        // 5. 将结果采集到控制台打印出来
        resultRDD.foreach(println)
        /*
        (15,(6120,1672,1259))
        (2,(6119,1767,1196))
        (20,(6098,1776,1244))
        (12,(6095,1740,1218))
        (11,(6093,1781,1202))
        (17,(6079,1752,1231))
        (7,(6074,1796,1252))
        (9,(6045,1736,1230))
        (19,(6044,1722,1158))
        (13,(6036,1781,1161))

Process finished with exit code 0

         */

        sc.stop()
    }
}
