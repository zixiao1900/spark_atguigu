package com.atguigu.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_filter {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - filter(bool)  filter之后可能会数据倾斜
        val rdd = sc.makeRDD(Range(0, 10), 2)  // 0 - 9
        rdd.saveAsTextFile("outputs/filter_input")
        // 0 1 2 3 4在一个分区   5 6 7 8 9在一个分区

        val filterRDD: RDD[Int] = rdd.filter(num=> num > 3)
        filterRDD.saveAsTextFile("outputs/filter_output")
        // 过滤之后4在一个分区  5 6 7 8 9在一个分区

        // todo 解决filter之后的数据倾斜 repartition
        val filterRDD1 = filterRDD.repartition(2)
        filterRDD1.saveAsTextFile("outputs/filter_output1")
        // repartition之后 4 6 8 在一个分区  5 7 9在一个分区  数据不再倾斜
        filterRDD1.collect().foreach(println)
        /*
        4
        5
        6
        7
        8
        9
         */


        sc.stop()

    }
}
