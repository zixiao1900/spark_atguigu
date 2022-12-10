package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_Output_foreachRDD_foreachPartition {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")

        val lines = ssc.socketTextStream("localhost", 9999)
        val wordToOne = lines.map((_,1))
        
        val windowDS: DStream[(String, Int)] =
            wordToOne.reduceByKeyAndWindow(
                (x:Int, y:Int) => { x + y},
                (x:Int, y:Int) => {x - y},
                Seconds(9), Seconds(3))

        // todo foreachRDD不会出现时间戳 拿到rdd
        // todo 连接不能写在driver层面(序列化问题)
        // todo 连接如果卸载foreach 则每个RDD中的每条数据都要创建 不合理
        // 所以要卸载foreach
        windowDS.foreachRDD(
            rdd => {
                // todo 数据库连接如果写在这里 就不能在后面的rdd.foreach中用 有序列化的问题
                rdd.foreach(tup => tup._2)
            }
        )

        windowDS.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    iter => {
                        // todo 这里连接数据库   iter.foreach是集合的方法 不存在序列化问题
                        // val conn = JDBCUtil.getConnection
                        iter.foreach(tup => tup._2)

                    }
                )
            }
        )



        ssc.start()
        ssc.awaitTermination()
    }

}
