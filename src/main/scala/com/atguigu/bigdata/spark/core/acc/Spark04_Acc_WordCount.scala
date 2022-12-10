package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Acc_WordCount {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
        val sc = new SparkContext(sparConf)

        // todo 自定义Map类型累加器
        val rdd = sc.makeRDD(List("hello", "spark", "hello"),2)

        // 累加器 : WordCount
        // todo 1. 创建累加器对象  自定义累加器
        val wcAcc = new MyAccumulator()
        // todo 2. 向Spark进行注册
        sc.register(wcAcc, "wordCountAcc")

        rdd.foreach(
            word => {
                // todo 3. 数据的累加（使用累加器）
                wcAcc.add(word)
            }
        )

        // todo 4. 获取累加器累加的结果
        println(wcAcc.value)  // Map(spark -> 1, hello -> 2)

        sc.stop()

    }
    /*
      自定义数据累加器：WordCount

      1. 继承AccumulatorV2, 定义泛型
         IN : 累加器输入的数据类型 String
         OUT : 累加器返回的数据类型 mutable.Map[String, Long]

      2. 重写方法（6）
     */
    //                                       输入类型    累加器返回类型
    class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

        // 累加器的值  Map保存当前每个词以及对应的次数
        private val wcMap = mutable.Map[String, Long]()

        // 判断是否初始状态
        override def isZero: Boolean = {
            wcMap.isEmpty
        }

        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
            new MyAccumulator()
        }

        override def reset(): Unit = {
            wcMap.clear()
        }

        // 获取累加器需要计算的值  新加入一个词 更新累加器
        override def add(word: String): Unit = {
            val newCnt = wcMap.getOrElse(word, 0L) + 1
            wcMap.update(word, newCnt)
        }

        // todo Driver合并多个累加器  map2的值 合并跟新到map1上
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {

            val map1 = this.wcMap
            val map2 = other.value

            map2.foreach{
                case ( word, count ) => {
                    val newCount = map1.getOrElse(word, 0L) + count
                    map1.update(word, newCount)
                }
            }
        }

        // 累加器结果  直接返回累加器的值
        override def value: mutable.Map[String, Long] = {
            wcMap
        }
    }
}
