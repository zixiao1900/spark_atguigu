package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_cache_toDebugString {

    def main(args: Array[String]): Unit = {

        // cache : 将数据临时存储在内存中进行数据重用
        //         todo 会在血缘关系中添加新的依赖。一旦，出现问题，可以重头读取数据
        // persist : 将数据临时存储在磁盘文件中进行数据重用
        //           涉及到磁盘IO，性能较低，但是数据安全
        //           如果作业执行完毕，临时保存的数据文件就会丢失
        // checkpoint : 将数据长久地保存在磁盘文件中进行数据重用
        //           涉及到磁盘IO，性能较低，但是数据安全
        //           为了保证数据安全，所以一般情况下，会独立执行作业
        //           为了能够提高效率，一般情况下，是需要和cache联合使用
        //           todo 执行过程中，会切断血缘关系。重新建立新的血缘关系
        //           checkpoint等同于改变数据源

        val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
        val sc = new SparkContext(sparConf)
        sc.setCheckpointDir("cp")

        val list = List("Hello Scala", "Hello Spark")

        val rdd = sc.makeRDD(list)

        val flatRDD = rdd.flatMap(_.split(" "))

        val mapRDD = flatRDD.map(word=>{
            (word,1)
        })
        println(mapRDD.toDebugString) // todo 持久化之前看一下血缘
        /*
        (1) MapPartitionsRDD[2] at map at Spark06_RDD_cache_toDebugString.scala:32 []
         |  MapPartitionsRDD[1] at flatMap at Spark06_RDD_cache_toDebugString.scala:30 []
         |  ParallelCollectionRDD[0] at makeRDD at Spark06_RDD_cache_toDebugString.scala:28 []
         */
        println("+++++++++++++")
        mapRDD.cache()
//        mapRDD.checkpoint()
        println(mapRDD.toDebugString) // todo 代码持久化 但是还没执行  持久化没起作用
        /*
        (1) MapPartitionsRDD[2] at map at Spark06_RDD_cache_toDebugString.scala:32 [Memory Deserialized 1x Replicated]
         |  MapPartitionsRDD[1] at flatMap at Spark06_RDD_cache_toDebugString.scala:30 [Memory Deserialized 1x Replicated]
         |  ParallelCollectionRDD[0] at makeRDD at Spark06_RDD_cache_toDebugString.scala:28 [Memory Deserialized 1x Replicated]
         */
        println("-----------------")
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        reduceRDD.collect().foreach(println)
        /*
        (Spark,1)
        (Hello,2)
        (Scala,1)
         */
        println("**************************************")
        println(mapRDD.toDebugString) // todo 持久化 并且执行之后 有了CachedPartitions 持久化起作用
        /*
        (1) MapPartitionsRDD[2] at map at Spark06_RDD_cache_toDebugString.scala:32 [Memory Deserialized 1x Replicated]
         |       CachedPartitions: 1; MemorySize: 368.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 0.0 B
         |  MapPartitionsRDD[1] at flatMap at Spark06_RDD_cache_toDebugString.scala:30 [Memory Deserialized 1x Replicated]
         |  ParallelCollectionRDD[0] at makeRDD at Spark06_RDD_cache_toDebugString.scala:28 [Memory Deserialized 1x Replicated]
         */
        mapRDD.unpersist()
        println(mapRDD.toDebugString) // todo 解除持久化之后 持久化不起作用了
        /*
        (1) MapPartitionsRDD[2] at map at Spark06_RDD_cache_toDebugString.scala:32 []
         |  MapPartitionsRDD[1] at flatMap at Spark06_RDD_cache_toDebugString.scala:30 []
         |  ParallelCollectionRDD[0] at makeRDD at Spark06_RDD_cache_toDebugString.scala:28 []
         */


        sc.stop()
    }
}
