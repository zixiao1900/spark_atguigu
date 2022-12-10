package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_glom_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - glom
        // todo 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
        val rdd : RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

        // 【1，2】，【3，4】
        val glomRDD: RDD[Array[Int]] = rdd.glom()
        // 【2】，【4】
        val maxRDD: RDD[Int] = glomRDD.map(
            (array: Array[Int]) => {
                array.max
            }
        )
        // 【6】
        println(maxRDD.collect().sum)





        sc.stop()

    }
}
