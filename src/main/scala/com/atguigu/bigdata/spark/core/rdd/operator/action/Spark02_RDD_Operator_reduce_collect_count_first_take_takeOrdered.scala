package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_reduce_collect_count_first_take_takeOrdered {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))

        // TODO - 行动算子

        // todo reduce
        val i: Int = rdd.reduce(_+_)
        println(i) // 10

        // todo collect : 方法会将不同分区的数据按照分区顺序采集到Driver端内存中，形成数组
        val ints: Array[Int] = rdd.collect()
        println(ints.mkString(",")) // 1,2,3,4

        // todo count : 数据源中数据的个数
        val cnt = rdd.count()
        println(cnt) // 4

        // todo first : 获取数据源中数据的第一个
        val first = rdd.first()
        println(first) // 1

        // todo take : 获取N个数据
        val ints_take: Array[Int] = rdd.take(3)
        println(ints_take.mkString(",")) // 1,2,3

        // todo takeOrdered : 数据排序后，取N个数据
        val rdd1 = sc.makeRDD(List(1,2,3,4))
        val ints1: Array[Int] = rdd1.takeOrdered(3)(Ordering.Int.reverse)
        println(ints1.mkString(",")) // 4,3,2

        sc.stop()

    }
}
