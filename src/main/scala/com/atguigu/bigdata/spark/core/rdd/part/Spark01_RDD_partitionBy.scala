package com.atguigu.bigdata.spark.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark01_RDD_partitionBy {

    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        // todo 数据按照自己的规则放入不同的分区

        val rdd = sc.makeRDD(List(
            ("nba", "xxxxxxxxx"),
            ("cba", "xxxxxxxxx"),
            ("wnba", "xxxxxxxxx"),
            ("nba", "xxxxxxxxx"),
        ),3)
        rdd.saveAsTextFile("outputs/part_default")
        // 0:nba,  1:cba,  2:wnba,nba
        val partRDD: RDD[(String, String)] = rdd.partitionBy( new MyPartitioner )
        partRDD.saveAsTextFile("outputs/part_myPart")
        // 0:nba,nba  1:wnba  2:cba



        sc.stop()
    }

    /**
      * 自定义分区器
      * 1. 继承Partitioner
      * 2. 重写方法
      */
    class MyPartitioner extends Partitioner {
        // todo 分区数量
        override def numPartitions: Int = 3

        // todo 根据数据的key 返回数据分区索引  从0开始
        override def getPartition(key: Any): Int = {
            key match {
                case "nba" => 0
                case "wnba" => 1
                case _ => 2
            }
        }
    }
}
