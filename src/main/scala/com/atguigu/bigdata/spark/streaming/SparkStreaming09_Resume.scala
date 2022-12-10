package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming09_Resume {

    def main(args: Array[String]): Unit = {

        // todo 流 从checkpoint恢复数据
        val ssc = StreamingContext.getActiveOrCreate(
            "cp1",  // 之前数据保存的检查点位置
            ()=>{ // 创建环境的对象
                val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
                val ssc = new StreamingContext(sparkConf, Seconds(3))

                val lines = ssc.socketTextStream("localhost", 9999)
                val wordToOne = lines.map((_,1))

                wordToOne.print()

                ssc
            }
        )


        ssc.checkpoint("cp")

        ssc.start()
        ssc.awaitTermination() // block 阻塞main线程


    }

}
