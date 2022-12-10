package com.atguigu.bigdata.spark.core.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01_RDD_toDebugString {

    def main(args: Array[String]): Unit = {
        //todo RDD不能存数据 保存血缘关系 一旦计算错误 可以通过血缘关系找到之前的源头 重新计算
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        val lines: RDD[String] = sc.textFile("datas/words.txt")
        println(lines.toDebugString) // 血缘关系
        /*
        (1) datas/words.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_toDebugString.scala:13 []
        |  datas/words.txt HadoopRDD[0] at textFile at Spark01_RDD_toDebugString.scala:13 []
         */
        println("*************************")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.toDebugString)
        /*
        (1) MapPartitionsRDD[2] at flatMap at Spark01_RDD_toDebugString.scala:16 []
         |  datas/words.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_toDebugString.scala:13 []
         |  datas/words.txt HadoopRDD[0] at textFile at Spark01_RDD_toDebugString.scala:13 []
         */
        println("*************************")
        val wordToOne = words.map(word=>(word,1))
        println(wordToOne.toDebugString)
        /*
        (1) MapPartitionsRDD[3] at map at Spark01_RDD_toDebugString.scala:19 []
         |  MapPartitionsRDD[2] at flatMap at Spark01_RDD_toDebugString.scala:16 []
         |  datas/words.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_toDebugString.scala:13 []
         |  datas/words.txt HadoopRDD[0] at textFile at Spark01_RDD_toDebugString.scala:13 []
         */
        println("*************************")
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
        println(wordToSum.toDebugString)
        /*
        (1) ShuffledRDD[4] at reduceByKey at Spark01_RDD_toDebugString.scala:22 []
         +-(1) MapPartitionsRDD[3] at map at Spark01_RDD_toDebugString.scala:19 []
            |  MapPartitionsRDD[2] at flatMap at Spark01_RDD_toDebugString.scala:16 []
            |  datas/words.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_toDebugString.scala:13 []
            |  datas/words.txt HadoopRDD[0] at textFile at Spark01_RDD_toDebugString.scala:13 []
         */
        println("*************************")
        val array: Array[(String, Int)] = wordToSum.collect()
        array.foreach(println)
        /*
        (scala,1)
        (spark,3)
        (hadoop,2)
        (hello,3)
         */

        sc.stop()

    }
}
