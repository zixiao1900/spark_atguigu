package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Operator_aggregateByKey_mapValues {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型)

        val rdd = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("b", 3),
            ("b", 4), ("b", 5), ("a", 6)
        ),2)

        // todo aggregateByKey最终的返回数据结果应该和初始值的类型保持一致
        //val aggRDD: RDD[(String, String)] = rdd.aggregateByKey("")(_ + _, _ + _)
        //aggRDD.collect.foreach(println)

        // todo 获取相同key的数据的平均值 => (a, 3),(b, 4)
        val newRDD : RDD[(String, (Int, Int))] = rdd.aggregateByKey( (0,0) )(
            ( t, v ) => { // todo  t是初始值是tuple(数值和， 次数)  v是对应key的value是Int  这里初始值是(Int, Int) 而计算的值是Int 不同
                (t._1 + v, t._2 + 1) // todo (数值和, 次数)  返回的结果类型和初始值一样是(Int, Int)
            },
            (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2)
            }
        )
        // todo mapValues  key不变 value改变
        val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
            case (num, cnt) => {
                num / cnt
            }
        }
        resultRDD.collect().foreach(println) // (b,4)(a,3)





        sc.stop()

    }
}
