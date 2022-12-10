package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_checkpoint {

    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local[*]").setAppName("checkpoint")
        val sc = new SparkContext(sparConf)
        sc.setCheckpointDir("checkPointDir/checkpoint")

        val list = List("Hello Scala", "Hello Spark")

        val rdd = sc.makeRDD(list, 1)

        val flatRDD = rdd.flatMap(_.split(" "))

        val mapRDD = flatRDD.map(word=>{
            println("@@@@@@@@@@@@")
            (word,1)
        })
        // todo checkpoint 需要落盘，需要指定检查点保存路径
        // todo 检查点路径保存的文件，当作业执行完毕后，不会被删除  persist如果保存在磁盘上会被自动删除
        // todo 一般保存路径都是在分布式存储系统：HDFS
        mapRDD.checkpoint()

        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        reduceRDD.collect().foreach(println)
        // todo checkpoint()会独立执行作业 @@@@是原来的两倍 job多了一次
        /*
            @@@@@@@@@@@@
            @@@@@@@@@@@@
            @@@@@@@@@@@@
            @@@@@@@@@@@@
            @@@@@@@@@@@@
            @@@@@@@@@@@@
            @@@@@@@@@@@@
            @@@@@@@@@@@@
            (Hello,2)
            (Scala,1)
            (Spark,1)
         */
        println("**************************************")
        val groupRDD = mapRDD.groupByKey()
        groupRDD.collect().foreach(println)
        /*
        (Hello,CompactBuffer(1, 1))
        (Scala,CompactBuffer(1))
        (Spark,CompactBuffer(1))
         */


        sc.stop()
    }
}
