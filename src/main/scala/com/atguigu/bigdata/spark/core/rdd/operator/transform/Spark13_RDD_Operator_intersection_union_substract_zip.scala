package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_intersection_union_substract_zip {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - 双Value类型

        // 交集，并集和差集要求两个数据源数据类型保持一致
        // 拉链操作两个数据源的类型可以不一致

        val rdd1 = sc.makeRDD(List(1,2,3,4))
        val rdd2 = sc.makeRDD(List(3,4,5,6))
        val rdd7 = sc.makeRDD(List("3","4","5","6"))

        // todo intersection交集 rdd1,rdd2数据类型要相同 : 【3，4】
        val rdd3: RDD[Int] = rdd1.intersection(rdd2)
//        val rdd9 = rdd1.intersection(rdd7)
        println(rdd3.collect().mkString(","))
        // 3,4

        // todo union并集  类型要相同: 【1，2，3，4，3，4，5，6】
        val rdd4: RDD[Int] = rdd1.union(rdd2)
        println(rdd4.collect().mkString(","))
        // 1,2,3,4,3,4,5,6

        // todo subtract差集 类型要相同 : 【1，2】
        val rdd5: RDD[Int] = rdd1.subtract(rdd2)
        println(rdd5.collect().mkString(",")) // 1,2

        // todo zip拉链 类型可以不同 分区数量要相同，分区中数据数量也要相同: 【1-3，2-4，3-5，4-6】
        val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
        val rdd8: RDD[(Int, String)] = rdd1.zip(rdd7)
        println(rdd6.collect().mkString(",")) // (1,3),(2,4),(3,5),(4,6)
        println(rdd8.collect().mkString(",")) // (1,3),(2,4),(3,5),(4,6)



        sc.stop()

    }
}
