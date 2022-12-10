package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
// todo 1号分区保留 其他分区不要
object Spark03_RDD_Operator_mapPartitionsWithIndex {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - mapPartitionsWithIndex 只对某个分区操作
        val rdd = sc.makeRDD(List(1,2,3,4), 2)
        // 【1，2】，【3，4】
        val mpiRDD = rdd.mapPartitionsWithIndex(
            (index, iter) => {
                if ( index == 1 ) {
                    iter
                } else {
                    Nil.iterator
                }
            }
        )
        mpiRDD.collect().foreach(println)
        // 3 4


        sc.stop()

    }
}
