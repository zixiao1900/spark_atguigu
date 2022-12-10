package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Req1_HotCategoryTop10Analysis_union {

    def main(args: Array[String]): Unit = {

        // TODO : Top10热门品类
        // 根据每个品类的点击，下单，支付的量统计热门品类top10
        val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparConf)

        // Q : actionRDD重复使用
        // Q : cogroup性能可能较低

        // 1. 读取原始日志数据
        val actionRDD = sc.textFile("datas/user_visit_action.txt")
        actionRDD.cache()

        // 2. 统计品类的点击数量：（品类ID，点击数量）
        val clickActionRDD = actionRDD.filter(
            action => {
                val datas = action.split("_")
                datas(6) != "-1"
            }
        )

        val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
            action => {
                val datas = action.split("_")
                (datas(6), 1)
            }
        ).reduceByKey(_ + _)

        // 3. 统计品类的下单数量：（品类ID，下单数量）
        val orderActionRDD = actionRDD.filter(
            action => {
                val datas = action.split("_")
                datas(8) != "null"
            }
        )

        // orderid => 1,2,3
        // 【(1,1)，(2,1)，(3,1)】
        val orderCountRDD = orderActionRDD.flatMap(
            action => {
                val datas = action.split("_")
                val cid = datas(8)
                val cids = cid.split(",")
                cids.map(id=>(id, 1))
            }
        ).reduceByKey(_+_)

        // 4. 统计品类的支付数量：（品类ID，支付数量）
        val payActionRDD = actionRDD.filter(
            action => {
                val datas = action.split("_")
                datas(10) != "null"
            }
        )

        // orderid => 1,2,3
        // 【(1,1)，(2,1)，(3,1)】
        val payCountRDD = payActionRDD.flatMap(
            action => {
                val datas = action.split("_")
                val cid = datas(10)
                val cids = cid.split(",")
                cids.map(id=>(id, 1))
            }
        ).reduceByKey(_+_)

        // (品类ID, 点击数量) => (品类ID, (点击数量, 0, 0))
        // (品类ID, 下单数量) => (品类ID, (0, 下单数量, 0))
        //                    => (品类ID, (点击数量, 下单数量, 0))
        // (品类ID, 支付数量) => (品类ID, (0, 0, 支付数量))
        //                    => (品类ID, (点击数量, 下单数量, 支付数量))
        // ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )

        // 5. 将品类进行排序，并且取前10名
        //    点击数量排序，下单数量排序，支付数量排序
        //    元组排序：先比较第一个，再比较第二个，再比较第三个，依此类推
        //    ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )
        //
        val rdd1 = clickCountRDD.map{
            case ( cid, clickCnt ) => {
                (cid, (clickCnt, 0, 0))
            }
        }
        val rdd2 = orderCountRDD.map{
            case ( cid, orderCnt ) => {
                (cid, (0, orderCnt, 0))
            }
        }
        val rdd3 = payCountRDD.map{
            case ( cid, payCnt ) => {
                (cid, (0, 0, payCnt))
            }
        }

        // 将三个数据源合并在一起，统一进行聚合计算
        val soruceRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)

        val analysisRDD: RDD[(String, (Int, Int, Int))] = soruceRDD.reduceByKey(
            ( t1, t2 ) => {
                ( t1._1+t2._1, t1._2 + t2._2, t1._3 + t2._3 )
            }
        )
        println(actionRDD.toDebugString) // 直到这里还没有触发actionRDD.cache
        val resultArray: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
        actionRDD.unpersist()

        // 6. 将结果采集到控制台打印出来
        resultArray.foreach(println)
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
         */

        sc.stop()
    }
}
