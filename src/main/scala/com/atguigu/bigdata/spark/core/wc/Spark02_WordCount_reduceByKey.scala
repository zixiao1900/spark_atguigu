package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount_reduceByKey {

  def main(args: Array[String]): Unit = {

    // Application
    // Spark框架
    // TODO 建立和Spark框架的连接
    // JDBC : Connection
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    // TODO 执行业务操作

    // 1. 读取文件，获取一行一行的数据
    //    hello world
    val lines: RDD[String] = sc.textFile("datas/words.txt")

    // 2. 将一行数据进行拆分，形成一个一个的单词（分词）
    //    扁平化：将整体拆分成个体的操作
    //   "hello world" => hello, world, hello, world
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 3. 将单词进行结构的转换,方便统计
    // word => (word, 1)r
    val wordToOne = words.map(word => (word, 1))

    // 4. 将转换后的数据进行分组聚合
    // 相同key的value进行聚合操作  reduceByKey相比groupByKey有预聚合功能
    // (word, 1) => (word, sum)   reduceByKey(_+_)
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey((x, y) => {x + y})

    // 5. 将转换结果采集到控制台打印出来  其实collect才会真正执行
    val array: Array[(String, Int)] = wordToSum.collect()
    array.foreach(println)


    // TODO 关闭连接
    sc.stop()

  }
}
