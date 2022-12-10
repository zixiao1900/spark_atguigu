package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_partitionBy {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO partitionBy算子 - (Key - Value类型)
        // todo 将数据按照指定 Partitioner 重新进行分区。Spark 默认的分区器是 HashPartitioner
        val rdd = sc.makeRDD(List(1,2,3,4),2)

        val mapRDD:RDD[(Int, Int)] = rdd.map((_,1))
        // RDD => PairRDDFunctions
        // 隐式转换（二次编译）

        // partitionBy根据指定的分区规则对数据进行重分区
        val newRDD = mapRDD.partitionBy(new HashPartitioner(2))

        // 这里新的分区器和老的类型和数量都相同 那么不会将数据重新分区
        mapRDD.partitionBy(new HashPartitioner(2))

        newRDD.saveAsTextFile("outputs/partitionBy")
        // 1,2  3,4  -> (2,1),(4,1)   (1,1),(3,1)




        sc.stop()

    }
}
