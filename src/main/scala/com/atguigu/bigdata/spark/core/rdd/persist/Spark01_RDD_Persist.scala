package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Persist {

    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)
        // todo 实现分组 + 聚合   和只分组两种方式
        val list = List("Hello Scala", "Hello Spark")

        val rdd = sc.makeRDD(list)

        val flatRDD = rdd.flatMap(_.split(" "))

        val mapRDD = flatRDD.map((_,1))

        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        // 分组 + 聚合
        reduceRDD.collect().foreach(println)
        println("**************************************")

        val rdd1 = sc.makeRDD(list)

        val flatRDD1 = rdd1.flatMap(_.split(" "))

        val mapRDD1 = flatRDD1.map((_,1))

        val groupRDD = mapRDD1.groupByKey()
        // 只分组
        groupRDD.collect().foreach(println)


        sc.stop()
    }
}
