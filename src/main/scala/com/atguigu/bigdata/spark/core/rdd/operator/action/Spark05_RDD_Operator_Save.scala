package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Save {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        //val rdd = sc.makeRDD(List(1,1,1,4),2)
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1),("a", 2),("a", 3)
        ),2)

        // TODO - 行动算子
        rdd.saveAsTextFile("outputs/saveAsTextFile")
        rdd.saveAsObjectFile("outputs/saveAsObjectFile")
        // todo saveAsSequenceFile方法要求数据的格式必须为K-V类型
        rdd.saveAsSequenceFile("outputs/saveAsSequenceFile")

        sc.stop()

    }
}
