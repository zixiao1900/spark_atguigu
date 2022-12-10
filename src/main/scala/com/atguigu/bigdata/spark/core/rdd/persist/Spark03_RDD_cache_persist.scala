package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_cache_persist {

    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
        val sc = new SparkContext(sparConf)
        // todo 持久化之后 重用RDD对象 就不会重新计算了
        // todo 代码里mapRDD.persist()之后不是立刻持久化 而是等用action算子之后 才真正持久化
        // todo 解除持久化的地方 一定要等重用的RDD用action算子执行后解除 才是对的  而不是mapRDD在代码里用过之后就可以 mapRDD.unpersist()
        // todo 持久化也不一定是为了重用 在数据比较长 或者数据比较重要的场合也可以采用持久化操作
        // todo unpersist是立刻解除持久化 不需要等行动算子 所以一定要等需要持久化的部分通过action算子之后 再解除 不要解除早了
        val list = List("Hello Scala", "Hello Spark")

        val rdd = sc.makeRDD(list)

        val flatRDD = rdd.flatMap(_.split(" "))

        val mapRDD = flatRDD.map(word=>{
            println("@@@@@@@@@@@@")
            (word,1)
        })
        // todo cache默认持久化的操作，只能将数据保存到内存中，如果想要保存到磁盘文件，需要更改存储级别
//        mapRDD.cache()

        // todo 持久化操作必须在行动算子执行时完成的。
        // todo 如果保存在磁盘上 也是临时保存 执行结束后是会自动删除的
        // todo 这里代码里虽然持久化了 但是还没起作用 要等action算子
        mapRDD.persist(StorageLevel.MEMORY_AND_DISK)

        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        reduceRDD.collect().foreach(println) // todo persist起作用的是这里
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
        val groupRDD = mapRDD.groupByKey()
//        mapRDD.unpersist() // todo 如果在这里解除持久化是立刻解除 持久化那就没起作用 一定要等重用的mapRDD用action算子执行之后再unpersist
        groupRDD.collect().foreach(println)
        mapRDD.unpersist() // todo 这里解除 才是正确的
        /* todo 这里没有@@@@@@@@@ mapRDD的结果直接groupByKey
        (Spark,CompactBuffer(1))
        (Hello,CompactBuffer(1, 1))
        (Scala,CompactBuffer(1))

         */


        sc.stop()
    }
}
