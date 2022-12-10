package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_sortBy {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - sortBy  有shuffle
        // todo sortBy默认情况下，不会改变分区。但是中间存在shuffle操作
        val rdd = sc.makeRDD(List(6,2,4,5,3,1), 2)

        val newRDD: RDD[Int] = rdd.sortBy(num=>num)

        newRDD.saveAsTextFile("outputs/sortBy")
        // 6，2，4   5，3，1  ->  1,2,3   4,5,6

        newRDD.collect().foreach(println)
        // 1 2 3 4 5 6



        sc.stop()

    }
}
