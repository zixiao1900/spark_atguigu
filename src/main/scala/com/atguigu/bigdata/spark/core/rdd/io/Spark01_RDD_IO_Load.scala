package com.atguigu.bigdata.spark.core.rdd.io

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Load {

    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        val rdd = sc.textFile("outputs/savesAsTextFile1")
        println(rdd.collect().mkString(","))

        val rdd1 = sc.objectFile[(String, Int)]("outputs/saveAsObjectFile1")
        println(rdd1.collect().mkString(","))

        // todo 键值类型
        val rdd2 = sc.sequenceFile[String, Int]("outputs/saveAsSequenceFile1")
        println(rdd2.collect().mkString(","))

        sc.stop()
    }
}
