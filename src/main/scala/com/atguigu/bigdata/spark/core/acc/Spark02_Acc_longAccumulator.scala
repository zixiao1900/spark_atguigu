package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc_longAccumulator {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
        val sc = new SparkContext(sparConf)

        val rdd = sc.makeRDD(List(1,2,3,4),2)

        // 获取系统累加器
        // 分布式共享只写变量  executor中的acc只有自己能访问 不能互相访问 最终会放回driver然后merge
        // Spark默认就提供了简单数据聚合的累加器
        val sumAcc = sc.longAccumulator("sum1")

//        sc.doubleAccumulator
//        sc.collectionAccumulator

        rdd.foreach(
            num => {
                // todo 在action算子中使用累加器
                // Executor每个分区做计算 结果返回Driver端再merge
                sumAcc.add(num)
            }
        )

        // 获取累加器的值
        println(sumAcc.value) // 10

        sc.stop()

    }
}
