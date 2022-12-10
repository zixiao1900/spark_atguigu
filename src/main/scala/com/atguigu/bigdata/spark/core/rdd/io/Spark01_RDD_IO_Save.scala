package com.atguigu.bigdata.spark.core.rdd.io

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Save {

    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        val rdd = sc.makeRDD(
            List(
                ("a", 1),
                ("b", 2),
                ("c", 3)
            ),
            2
        )

        rdd.saveAsTextFile("outputs/savesAsTextFile1")
        rdd.saveAsObjectFile("outputs/saveAsObjectFile1")
        // todo 必须是键值类型
        rdd.saveAsSequenceFile("outputs/saveAsSequenceFile1")

        sc.stop()
    }
}
