package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_WordCount_final {
  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val groupBy_res: String = wordcount_groupBy(sc)
    println("groupBy:")
    println(groupBy_res) // (Spark,1),(Hello,2),(Scala,1)

    val groupByKey_res: String = wordcount_groupByKey(sc)
    println("groupByKey:")
    println(groupByKey_res) // (Spark,1),(Hello,2),(Scala,1)

    val reduceByKey_res: String = wordcount_reduceByKey(sc)
    println("reduceByKey:")
    println(reduceByKey_res) // (Spark,1),(Hello,2),(Scala,1)

    val aggregateByKey_res: String = wordcount_aggregateByKey(sc)
    println("aggregateByKey:")
    println(aggregateByKey_res) // (Spark,1),(Hello,2),(Scala,1)

    val foldByKey_res: String = wordcount_foldByKey(sc)
    println("foldByKey:")
    println(foldByKey_res) // (Spark,1),(Hello,2),(Scala,1)

    val combineByKey_res: String = wordcount_combineByKey(sc)
    println("combineByKey:")
    println(combineByKey_res) // (Spark,1),(Hello,2),(Scala,1)

    val countByKey_res: collection.Map[String, Long] = wordcount_countByKey(sc)
    println("countByKey:")
    println(countByKey_res) // Map(Spark -> 1, Hello -> 2, Scala -> 1)

    val countByValue_res: collection.Map[String, Long] = wordcount_countByValue(sc)
    println("countByValue:")
    println(countByValue_res) // Map(Spark -> 1, Hello -> 2, Scala -> 1)

    val reduce_res: collection.Map[String, Long] = wordcount_reduce(sc)
    println("reduce:")
    println(reduce_res) // Map(Hello -> 2, Scala -> 1, Spark -> 1)

    sc.stop()

  }

  // todo groupBy transformer
  def wordcount_groupBy(sc: SparkContext) = {

    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
    wordCount.collect().mkString(",")
  }

  // todo groupByKey transformer
  def wordcount_groupByKey(sc: SparkContext)= {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
    wordCount.collect().mkString(",")
  }

  // todo reduceByKey transformer
  def wordcount_reduceByKey(sc: SparkContext) = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
    wordCount.collect().mkString(",")
  }

  // todo aggregateByKey  transformer
  def wordcount_aggregateByKey(sc: SparkContext) = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_ + _, _ + _)
    wordCount.collect().mkString(",")
  }

  // todo foldByKey  transformer
  def wordcount_foldByKey(sc: SparkContext)= {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_ + _)
    wordCount.collect().mkString(",")
  }

  // todo combineByKey  transformer
  def wordcount_combineByKey(sc: SparkContext) = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.combineByKey(
      v => v,
      (x: Int, y) => x + y,
      (x: Int, y: Int) => x + y
    )
    wordCount.collect().mkString(",")
  }

  // todo countByKey action
  def wordcount_countByKey(sc: SparkContext)= {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount: collection.Map[String, Long] = wordOne.countByKey()
    wordCount
  }

  // todo countByValue  action
  def wordcount_countByValue(sc: SparkContext)= {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordCount: collection.Map[String, Long] = words.countByValue()
    wordCount
  }

  // todo reduce action
  def wordcount_reduce(sc: SparkContext)= {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))

    // 【（word, count）,(word, count)】
    // word => Map[(word,1)]
    val mapWord: RDD[mutable.Map[String, Long]] = words.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )
    // todo Map reduce合并
    val wordCount = mapWord.reduce(
      (map1, map2) => {
        map2.foreach {
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
          }
        }
        map1
      }
    )

    wordCount
  }

}
