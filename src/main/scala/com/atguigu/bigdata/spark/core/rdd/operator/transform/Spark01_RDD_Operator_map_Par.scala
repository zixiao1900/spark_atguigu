package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_map_Par {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - map

        // 1. rdd的计算一个分区内的数据是一个一个执行逻辑
        //    只有前面一个数据全部的逻辑执行完毕后，才会执行下一个数据。
        //    分区内数据的执行是有序的。
        // 2. 不同分区数据计算是无序的。
        // 1,2是一个分区  3，4是一个 只能保证 1在2前 3在4前 其他顺序无法保证
        val rdd = sc.makeRDD(List(1,2,3,4),2)

        val mapRDD = rdd.map(
            num => {
                println(">>>>>>>> " + num)
                num
            }
        )
        val mapRDD1 = mapRDD.map(
            num => {
                println("######" + num)
                num
            }
        )

        mapRDD1.collect()
        /* 分区内有序  分区间无序  1一定在2之前  3一定在4之前 其他顺序无法保证
        >>>>>>>> 1
        >>>>>>>> 3
        ######3
        ######1
        >>>>>>>> 4
        ######4
        >>>>>>>> 2
        ######2
         */

        sc.stop()

    }
}
