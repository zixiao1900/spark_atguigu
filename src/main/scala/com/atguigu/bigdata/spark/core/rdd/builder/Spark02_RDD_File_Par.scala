package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par {

    def main(args: Array[String]): Unit = {

        // TODO 准备环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD
        // textFile可以将文件作为数据处理的数据源，默认也可以设定分区。
        //     minPartitions : 最小分区数量
        //     math.min(defaultParallelism, 2)
        //val rdd = sc.textFile("datas/1.txt")
        // 如果不想使用默认的分区数量，可以通过第二个参数指定分区数
        // Spark读取文件，底层其实使用的就是Hadoop的读取方式
        // 分区数量的计算方式：
        //    totalSize = 58  一共58个字节
        //    goalSize =  58 / 3 = 19（byte） 每个分区存17个字节

        //    58 / 3 = 19...1   1/19 < 1/10 所以不会再产生分区 一共就是3个分区 这里如果余数 n/19 > 1/10 就会增加一个分区 共4个分区

        //
        val rdd = sc.textFile("datas/words.txt", 3)

        rdd.saveAsTextFile("outputs/words")


        // TODO 关闭环境
        sc.stop()
    }
}
