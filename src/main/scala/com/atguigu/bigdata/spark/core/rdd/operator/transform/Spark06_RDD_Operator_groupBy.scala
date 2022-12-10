package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_groupBy {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - groupBy
        val rdd : RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

        // groupBy会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
        // 相同的key值的数据会放置在一个组中
        // 数据根据指定的规则进行分组，分区默认不变，但是数据会被打乱重新组合
        // 一个组的数据在一个分区中，但是并不是说一个分区中只有一个组
        def groupFunction(num:Int) = {
            num % 2 // 奇数偶数分组
        }

        val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)
//        val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(num => num % 2)
//        groupRDD.saveAsTextFile("outputs/groupBy")
        groupRDD.collect().foreach(println)
        /*
        (0,CompactBuffer(2, 4))
        (1,CompactBuffer(1, 3))
        以上两条数据可能在一个分区中 也可能在两个不同的分区中
         */


        sc.stop()

    }
}
