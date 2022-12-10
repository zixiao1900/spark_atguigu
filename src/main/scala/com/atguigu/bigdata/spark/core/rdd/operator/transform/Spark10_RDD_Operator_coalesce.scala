package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_coalesce {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - coalesce缩减分区
        val rdd = sc.makeRDD(List(1,2,3,4,5,6), 3)
        rdd.saveAsTextFile("outputs/coaleasce_in")
        // 1,2   3,4   5,6

        // coalesce方法默认情况下不会将分区的数据打乱重新组合
        // 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
        // todo 如果想要让数据均衡，可以进行shuffle处理
        //val newRDD: RDD[Int] = rdd.coalesce(2)
        val newRDD: RDD[Int] = rdd.coalesce(2, shuffle = true)

        newRDD.saveAsTextFile("outputs/coaleasce_out")
        // 1,4,5   2,3,6




        sc.stop()

    }
}
