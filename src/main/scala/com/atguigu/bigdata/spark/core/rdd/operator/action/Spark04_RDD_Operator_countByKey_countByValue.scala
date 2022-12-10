package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_countByKey_countByValue {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(List(1,1,1,4),2)
        val rdd2 = sc.makeRDD(List(
            ("a", 1),("a", 2),("a", 2),("b", 5)
        ))

        // TODO - 行动算子
        // todo countByValue单值类型数据 每个数据出现的次数统计 返回Map
        val intToLong: collection.Map[Int, Long] = rdd1.countByValue()
        println(intToLong) // Map(4 -> 1, 1 -> 3)

        // todo k-v类型 countByKey  返回每个key出现的次数
        val stringToLong: collection.Map[String, Long] = rdd2.countByKey()
        println(stringToLong) // Map(a -> 3, b -> 1)

        // todo k-v类型 countByValue 返回没对k-v 出现的次数
        val tupleToLong: collection.Map[(String, Int), Long] = rdd2.countByValue()
        println(tupleToLong) // Map((a,1) -> 1, (b,5) -> 1, (a,2) -> 2)
        sc.stop()

    }
}
