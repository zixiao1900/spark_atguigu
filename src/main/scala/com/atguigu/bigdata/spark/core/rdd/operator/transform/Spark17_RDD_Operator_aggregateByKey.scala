package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_aggregateByKey {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO aggregateByKey 算子 - (Key - Value类型)
        // todo 分区内，分区间可以用不同的计算规则  还需要指定分区内计算的初始值

        val rdd = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("a", 3), ("a", 4)
        ),2)
        // (a,【1,2】), (a, 【3，4】)
        // (a, 2), (a, 4)
        // (a, 6)

        // aggregateByKey存在函数柯里化，有两个参数列表
        // 第一个参数列表,需要传递一个参数，表示为初始值
        //       主要用于当碰见第一个key的时候，和value进行分区内计算
        // 第二个参数列表需要传递2个参数
        //      第一个参数表示分区内计算规则
        //      第二个参数表示分区间计算规

        // math.min(x, y)
        // math.max(x, y)
        rdd.aggregateByKey(0)( // 分区内的初始值
            (x, y) => math.max(x, y), // 分区内取最大值
            (x, y) => x + y // 分区间求和
        ).collect.foreach(println)
        /*
        // (a,【1,2】), (a, 【3，4】)
        // (a, 2), (a, 4)
        // (a, 6)
         */





        sc.stop()

    }
}
