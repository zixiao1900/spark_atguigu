package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Operator_leftOuterJoin {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO leftOuterJoin

        val rdd1 = sc.makeRDD(List(
            ("a", 1), ("b", 2),("d",1)//, ("c", 3)
        ))

        val rdd2 = sc.makeRDD(List(
            ("a", 4), ("b", 5),("c", 6)
        ))
        val leftJoinRDD = rdd1.leftOuterJoin(rdd2)
//        val rightJoinRDD = rdd1.rightOuterJoin(rdd2)

        leftJoinRDD.collect().foreach(println)
//        rightJoinRDD.collect().foreach(println)
        /*
        (a,(1,Some(4)))
        (b,(2,Some(5)))
        (d,(1,None))

         */



        sc.stop()

    }
}
