package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark06_Bc {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
        val sc = new SparkContext(sparConf)

        val rdd1 = sc.makeRDD(List(
            ("a", 1),("b", 2),("c", 3)
        ),2)
        // todo 这里map1的值 定义在driver端  在rdd算子中不需要修改 只需要读  这种情况下 可以定义为广播变量
        val map1 = mutable.Map(("a", 4),("b", 5),("c", 6))

        // todo 封装广播变量
        // todo 将driver端的闭包数据 保存在executor的内存中
        // todo 如果该executor中有多个task 可以共享只读executor内存中的闭包数据
        // todo 广播变量只读 不能改
        val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map1)

        rdd1.map {
            case (w, c) => {
                // 方法广播变量  只读
                val l: Int = bc.value.getOrElse(w, 0)
                (w, (c, l))
            }
        }.collect().foreach(println)



        sc.stop()

    }
}
