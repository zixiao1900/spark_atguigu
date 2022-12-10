package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req1_HotCategoryTop10Analysis_cogroup {

    def main(args: Array[String]): Unit = {

        // TODO : Top10热门品类
        // 根据每个品类的点击，下单，支付的量统计热门品类top10
        val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparConf)

        // 1. 读取原始日志数据
        val actionRDD = sc.textFile("datas/user_visit_action.txt")
        actionRDD.cache()


        // 2. 统计品类的点击数量：（品类ID，点击数量）
        val clickActionRDD = actionRDD.filter(
            action => { // 过滤 只要点击行为数据
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
        val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(
            action => {
                val datas = action.split("_")
                val cid = datas(10)
                val cids = cid.split(",")
                cids.map(id=>(id, 1))
            }
        ).reduceByKey(_+_)

        // 5. 将品类进行排序，并且取前10名
        //    点击数量排序，下单数量排序，支付数量排序
        //    元组排序：先比较第一个，再比较第二个，再比较第三个，依此类推
        //    ( 品类ID, ( 点击数量, 下单数量, 支付数量 )
        //  cogroup = connect + group
        val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
            clickCountRDD.cogroup(orderCountRDD, payCountRDD)
//        cogroupRDD.collect().take(5).foreach(println)
        /*
        (4,(CompactBuffer(5961),CompactBuffer(1760),CompactBuffer(1271)))
        (8,(CompactBuffer(5974),CompactBuffer(1736),CompactBuffer(1238)))
        (20,(CompactBuffer(6098),CompactBuffer(1776),CompactBuffer(1244)))
        (19,(CompactBuffer(6044),CompactBuffer(1722),CompactBuffer(1158)))
        (15,(CompactBuffer(6120),CompactBuffer(1672),CompactBuffer(1259)))
         */
//        println("************")
        val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues{
            case ( clickIter, orderIter, payIter ) => {
                // 统计click数量
                var clickCnt = 0
                val iter1 = clickIter.iterator
                if ( iter1.hasNext ) {
                    clickCnt = iter1.next()
                }
                // 统计order数量
                var orderCnt = 0
                val iter2 = orderIter.iterator
                if ( iter2.hasNext ) {
                    orderCnt = iter2.next()
                }
                // 统计pay数量
                var payCnt = 0
                val iter3 = payIter.iterator
                if ( iter3.hasNext ) {
                    payCnt = iter3.next()
                }

                ( clickCnt, orderCnt, payCnt )
            }
        }
        // 到这里位置 采用action算子take触发了actionRDD.cache操作
        val resultArray: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
        // 这里才能unpersist
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
