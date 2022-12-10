package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {

    def main(args: Array[String]): Unit = {

        // TODO 创建环境对象
        // StreamingContext创建时，需要传递两个参数
        // 第一个参数表示环境配置
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        // 第二个参数表示批量处理的周期（采集周期）
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // TODO 逻辑处理
        // 获取端口数据
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        val words: DStream[String] = lines.flatMap(_.split(" "))

        val wordToOne: DStream[(String, Int)] = words.map((_,1))

        // 一个采集周期内的做为一个微批次 做聚合wordCount
        val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_+_)

        // print会打印分割线和时间戳
        wordToCount.print()

        // 由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
        // 如果main方法执行完毕，应用程序也会自动结束。所以不能让main执行完毕
//        ssc.stop()
        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭   采集器不关 环境就不停
        ssc.awaitTermination()
    }
}

// todo
//  1 先运行程序
//  2 cmd: nc -lp 9999
//  3输入数据流进行测试
