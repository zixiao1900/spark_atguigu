package com.atguigu.bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_dependencies {

    def main(args: Array[String]): Unit = {


        val sparConf = new SparkConf().setMaster("local").setAppName("Dep")
        val sc = new SparkContext(sparConf)

        // todo OneToOneDependency窄依赖 新的RDD的一个分区数据依赖预旧的RDD一个分区
        // todo ShuffleDependency宽依赖 新的RDD一个分区依赖于旧的RDD的多个分区

        val lines: RDD[String] = sc.textFile("datas/words.txt")
        println(lines.dependencies)
        //  List(org.apache.spark.OneToOneDependency@671d1157)
        println("*************************")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.dependencies)
        // List(org.apache.spark.OneToOneDependency@372ca2d6)
        println("*************************")
        val wordToOne = words.map(word=>(word,1))
        println(wordToOne.dependencies)
        // List(org.apache.spark.OneToOneDependency@10b1a751)
        println("*************************")
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
        println(wordToSum.dependencies)
        // List(org.apache.spark.ShuffleDependency@5d7399f9)
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
