package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
        val sc = new SparkContext(sparConf)

        val rdd = sc.makeRDD(List(1,2,3,4),2)

        // reduce : 分区内计算，分区间计算
        val i: Int = rdd.reduce(_+_)
        println(i) // 10
        var sum = 0
        rdd.foreach(
            num => {
                // todo executor端计算 并不会返回
                // 分区1. sum_1: 0+1 -> 1+2 -> 3
                // 分区2. sum_2: 0+3 -> 3+4 -> 7
                // sum_1, sum_2都不会从executor传回driver所以driver端的sum依然是0
                sum += num
            }
        )
        // todo 不会加上
        println("sum = " + sum) // 0

        sc.stop()

    }
}
