package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_aggregate_fold {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4),2)

        // TODO - 行动算子

        //10 + 13 + 17 = 40
        // todo aggregateByKey : 初始值只会参与分区内计算
        // todo aggregate : 初始值会参与分区内计算,并且和参与分区间计算
        val result: Int = rdd.aggregate(10)(_+_, _+_) // 40
        val result1: Int = rdd.fold(10)(_+_) // 40

        println(result)
        println(result1)

        sc.stop()

    }
}
