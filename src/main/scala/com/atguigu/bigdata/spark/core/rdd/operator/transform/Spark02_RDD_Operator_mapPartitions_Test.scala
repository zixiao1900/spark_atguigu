package com.atguigu.bigdata.spark.core.rdd.operator.transform
// todo 找到每个分区的最大值

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_mapPartitions_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - mapPartitions
        val rdd = sc.makeRDD(List(1,2,3,4), 2)

        // 【1，2】，【3，4】
        // 【2】，【4】
        val mpRDD = rdd.mapPartitions(
            iter => {
                List(iter.max).iterator // iter.max是Int 用List.iterator包成迭代器返回
            }
        )
        mpRDD.collect().foreach(println)
        // 2 4

        sc.stop()

    }
}
