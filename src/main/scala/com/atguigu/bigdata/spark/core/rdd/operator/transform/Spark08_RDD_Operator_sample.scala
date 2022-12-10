package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Operator_sample {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - sample抽样本
        val rdd = sc.makeRDD(Range(0, 11))
        // 0 - 10

        // sample算子需要传递三个参数
        // 1. 第一个参数表示，抽取数据后是否将数据返回 true（放回），false（丢弃）
        // 2. 第二个参数表示，
        //         如果抽取不放回的场合：数据源中每条数据被抽取的概率，基准值的概念
        //         如果抽取放回的场合：表示数据源中的每条数据被抽取的可能次数
        // 3. 第三个参数表示，抽取数据时随机算法的种子
        //                    如果不传递第三个参数，那么使用的是当前系统时间
        println(rdd.sample(
            false, // 不放回
            0.4, // 每条数据被抽取的概率  不一定10条一定抽4条
            1 // 设置种子
        ).collect().mkString(","))

        val sampleRDD: RDD[Int] = rdd.sample(
            true, // 可放回
            2, // 每条数据被抽取的可能次数
            1
        )
        println(sampleRDD.collect().mkString(","))


        sc.stop()

    }
}
