package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
// todo 展示数值来源于哪个分区
object Spark03_RDD_Operator_mapPartitionsWithIndex1 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - mapPartitions
        val rdd = sc.makeRDD(List(1,2,3,4), 2)

        val mpiRDD = rdd.mapPartitionsWithIndex(
            (index, iter) => {
                // 1,   2,    3,   4
                //(0,1)(0,2),(1,3),(1,4)
                iter.map(
                    num => {
                        (index, num)
                    }
                )
            }
        )

        mpiRDD.collect().foreach(println)
        /*
        (0,1)
        (0,2)
        (1,3)
        (1,4)
         */


        sc.stop()

    }
}
