package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Req1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("req1")
    val sc = new SparkContext(sparkConf)
    // TODO 案例实操 agent.log   时间戳  省份  城市 用户 广告
    // todo 求每个省份 每个广告被点击数量的top3
    val resultRDD = sc.textFile("datas/agent.log")
      .map(line=>{
        val datas: Array[String] = line.split("\\s+")
        ((datas(1), datas(4)), 1)
      }).reduceByKey(_+_)
      .map{
        case ((prov, ad), sum_) => (prov, (ad, sum_))
      }.groupByKey()
      .mapValues(
        iter => {
          iter.toList.sortBy(t=>t._2)(Ordering.Int.reverse).take(3)
        }
      )
    resultRDD.collect().foreach(println)
    /*
    (4,List((12,25), (2,22), (16,22)))
    (8,List((2,27), (20,23), (11,22)))
    (6,List((16,23), (24,21), (22,20)))
    (0,List((2,29), (24,25), (26,24)))
    (2,List((6,24), (21,23), (29,20)))
    (7,List((16,26), (26,25), (1,23)))
    (5,List((14,26), (21,21), (12,21)))
    (9,List((1,31), (28,21), (0,20)))
    (3,List((14,28), (28,27), (22,25)))
    (1,List((3,25), (6,23), (5,22)))

Process finished with exit code 0

     */

  }

}
