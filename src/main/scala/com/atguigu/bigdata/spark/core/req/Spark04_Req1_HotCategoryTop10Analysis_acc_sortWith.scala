package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Req1_HotCategoryTop10Analysis_acc_sortWith {

    def main(args: Array[String]): Unit = {

        // TODO : Top10热门品类
        // todo 不用reduceByKey 直接用累加器统计次数
        val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparConf)

        // 1. 读取原始日志数据
        val actionRDD = sc.textFile("datas/user_visit_action.txt")

        val acc = new HotCategoryAccumulator
        sc.register(acc, "hotCategory")

        // 2. 将数据转换结构
        actionRDD.foreach(
            action => {
                val datas = action.split("_")
                if (datas(6) != "-1") {
                    // 点击的场合
                    acc.add((datas(6), "click"))
                }
                else if (datas(8) != "null") {
                    // 下单的场合
                    val ids: Array[String] = datas(8).split(",")
                    ids.foreach(
                        id => {
                            acc.add( (id, "order") )
                        }
                    )
                }
                else if (datas(10) != "null") {
                    // 支付的场合
                    val ids: Array[String] = datas(10).split(",")
                    ids.foreach(
                        id => {
                            acc.add( (id, "pay") )
                        }
                    )
                }
            }
        )
        // todo 累加器结果
        val accVal: mutable.Map[String, HotCategory] = acc.value
        // key其实在HotCategory中也有  这里直接取value
        val categories: Iterable[HotCategory] = accVal.values

        val sort: List[HotCategory] = categories.toList.sortWith(
            (left, right) => {
                if ( left.clickCnt > right.clickCnt ) {
                    true
                }
                else if (left.clickCnt == right.clickCnt) {

                    if ( left.orderCnt > right.orderCnt ) {
                        true
                    }
                    else if (left.orderCnt == right.orderCnt) {
                        left.payCnt > right.payCnt
                    }
                    else {
                        false
                    }
                } else {
                    false
                }
            }
        )

        // 5. 将结果采集到控制台打印出来
        sort.take(10).foreach(println)
        /*
        HotCategory(15,6120,1672,1259)
        HotCategory(2,6119,1767,1196)
        HotCategory(20,6098,1776,1244)
        HotCategory(12,6095,1740,1218)
        HotCategory(11,6093,1781,1202)
        HotCategory(17,6079,1752,1231)
        HotCategory(7,6074,1796,1252)
        HotCategory(9,6045,1736,1230)
        HotCategory(19,6044,1722,1158)
        HotCategory(13,6036,1781,1161)
         */

        sc.stop()
    }
    // TODO 用var是后面要作为可变参数
    case class HotCategory( cid:String, var clickCnt : Int, var orderCnt : Int, var payCnt : Int )
    /**
      * 自定义累加器
      * 1. 继承AccumulatorV2，定义泛型
      *    IN : ( 品类ID, 行为类型 )
      *    OUT : mutable.Map[String, HotCategory]
      * 2. 重写方法（6）
      */
    //                                                  CateID   TYPE
    class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]{

        private val hcMap = mutable.Map[String, HotCategory]()

        override def isZero: Boolean = {
            hcMap.isEmpty
        }

        override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
            new HotCategoryAccumulator()
        }

        override def reset(): Unit = {
            hcMap.clear()
        }

        override def add(v: (String, String)): Unit = {
            val cid = v._1
            val actionType = v._2
            val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0,0,0))
            if ( actionType == "click" ) {
                category.clickCnt += 1
            }
            else if (actionType == "order") {
                category.orderCnt += 1
            }
            else if (actionType == "pay") {
                category.payCnt += 1
            }
            hcMap.update(cid, category)
        }

        override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
            // todo 将map2内容 merge到map1
            val map1 = this.hcMap
            val map2 = other.value

            map2.foreach{
                case ( cid, hc ) => {
                    val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0,0,0))
                    category.clickCnt += hc.clickCnt
                    category.orderCnt += hc.orderCnt
                    category.payCnt += hc.payCnt
                    map1.update(cid, category)
                }
            }
        }

        override def value: mutable.Map[String, HotCategory] = hcMap
    }
}
