package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Persist {

    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
        val sc = new SparkContext(sparConf)
        // todo 因为RDD不保存数据 即使重用RDD对象 也是需要从头计算
        val list = List("Hello Scala", "Hello Spark")

        val rdd = sc.makeRDD(list)

        val flatRDD = rdd.flatMap(_.split(" "))
        // todo 虽然mapRDD这个对象重用了 RDD中不存数据 实际上还是根据血缘从头走计算了一遍
        val mapRDD = flatRDD.map(word=>{
            println("@@@@@@@@@@@@")
            (word,1)
        })

        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        reduceRDD.collect().foreach(println)
        /*
        @@@@@@@@@@@@
        @@@@@@@@@@@@
        @@@@@@@@@@@@
        @@@@@@@@@@@@
        (Spark,1)
        (Hello,2)
        (Scala,1)
         */
        println("**************************************")
        // todo 虽然mapRDD这个对象重用了 但是实际上还是根据血缘从头走计算了一遍
        val groupRDD = mapRDD.groupByKey()
        groupRDD.collect().foreach(println)
        /*
        @@@@@@@@@@@@
        @@@@@@@@@@@@
        @@@@@@@@@@@@
        @@@@@@@@@@@@
        (Spark,CompactBuffer(1))
        (Hello,CompactBuffer(1, 1))
        (Scala,CompactBuffer(1))
         */


        sc.stop()
    }
}
