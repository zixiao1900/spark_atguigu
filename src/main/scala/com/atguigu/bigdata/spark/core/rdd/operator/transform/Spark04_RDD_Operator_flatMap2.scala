package com.atguigu.bigdata.spark.core.rdd.operator.transform
// todo 将[[1,2],3,[4,5]]打平成1,2,3,4,5 用了模式匹配
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_flatMap2 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - flatMap
        val rdd = sc.makeRDD(List(List(1,2),3,List(4,5)))

        val flatRDD = rdd.flatMap {
            data => {
                data match {
                    case data: List[_] => data
                    case data => List(data)
                }
            }
        }

        flatRDD.collect().foreach(println)
        /*
        1
        2
        3
        4
        5
         */



        sc.stop()

    }
}
