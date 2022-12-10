package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount_groupByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount2")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("datas/words.txt")

    val words: RDD[String] = lines.flatMap(line => line.split("\\s+"))

    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))

    val wordGroup1: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)
    /*
    (scala,CompactBuffer((scala,1)))
    (hello,CompactBuffer((hello,1), (hello,1), (hello,1)))
    (spark,CompactBuffer((spark,1), (spark,1), (spark,1)))
    (hadoop,CompactBuffer((hadoop,1)))
     */

    val wordGroup2: RDD[(String, Iterable[Int])] = wordToOne.groupByKey()
    /*
    (scala,CompactBuffer(1))
    (hello,CompactBuffer(1, 1, 1))
    (spark,CompactBuffer(1, 1, 1))
    (hadoop,CompactBuffer(1))
     */

    val wordCount: RDD[(String, Int)] = wordGroup1.map(
      tup => {
        tup._2.reduce(
          (t1, t2) => {
            (tup._1, t1._2 + t2._2)  // tup._1换成t1._1也可以
          }
        )
      }
    )
    wordCount.collect().foreach(println)

    sc.stop()
  }
}
