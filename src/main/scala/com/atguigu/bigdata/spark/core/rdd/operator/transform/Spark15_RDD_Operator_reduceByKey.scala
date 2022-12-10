package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_RDD_Operator_reduceByKey {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // todo reduceByKey算子 - (Key - Value类型)
        // todo 存在shuffle 分组之前(落盘之前) 分区内先预聚合了 性能比groupByKey好
        // todo 分区内的预聚合规则和分区间聚合规则完全相同

        val rdd = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("a", 3), ("b", 4)
        ))

        // reduceByKey : 相同的key的数据进行value数据的聚合操作
        // scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合
        // 【1，2，3】
        // 【3，3】
        // 【6】
        // reduceByKey中如果key的数据只有一个，是不会参与运算的。
        val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey( (x:Int, y:Int) => {
            println(s"x = ${x}, y = ${y}")
            x + y
        } )

        reduceRDD.collect().foreach(println)
        /*
        x = 1, y = 2
        x = 3, y = 3
        (a,6)
        (b,4)
         */




        sc.stop()

    }
}
