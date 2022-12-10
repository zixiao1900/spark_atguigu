package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount_groupBy {

  def main(args: Array[String]): Unit = {

    // Application
    // Spark框架
    // TODO 建立和Spark框架的连接
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount") // 环境
    val sc = new SparkContext(sparConf) // 连接

    // TODO 执行业务操作

    // 1. 读取文件，获取一行一行的数据
    //    hello world
    val lines: RDD[String] = sc.textFile("datas/words.txt")

    // 2. 将一行数据进行拆分，形成一个一个的单词（分词）
    //    扁平化：将整体拆分成个体的操作
    //   "hello world" => hello, world, hello, world
    val words: RDD[String] = lines.flatMap((line: String) => line.split(" "))

    // 3. 将数据根据单词进行分组，便于统计
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    /*  groupBy
    (scala,CompactBuffer(scala))
    (spark,CompactBuffer(spark))
    (hadoop,CompactBuffer(hadoop))
    (hello,CompactBuffer(hello, hello, hello))
     */

    // 4. 对分组后的数据进行转换
    //    (hello, hello, hello), (world, world)
    //    (hello, 3), (world, 2)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    // 不同case也可以
//    val wordToCount = wordGroup.map(
//      tup => (tup._1, tup._2.size)
//    )

    // 5. 将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    // TODO 关闭连接
    sc.stop()

  }
}
