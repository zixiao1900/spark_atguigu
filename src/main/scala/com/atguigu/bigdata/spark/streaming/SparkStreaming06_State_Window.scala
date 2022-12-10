package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Window {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        val wordToOne: DStream[(String, Int)] = lines.map((_,1))

        // todo updateStateByKey是从数据流开始直到当前数据流 所有的数据做wordCount统计
        // todo 窗口的范围应该是采集周期的整数倍   比如 可以对最近2个采集周期 做wordCount统计
        // todo 窗口可以滑动的，但是默认情况下，一个采集周期进行滑动
        // 这样的话，可能会出现重复数据的计算，为了避免这种情况，可以改变滑动的滑动（步长）
        val windowDS: DStream[(String, Int)] = wordToOne.window(
            Seconds(6), // 窗口大小 必须是采集周期的整数倍
            Seconds(6)  // 步长，默认是采集周期 如果设置为窗口大小，就不会计算重复数据
        )

        val wordToCount: DStream[(String, Int)] = windowDS.reduceByKey(_+_)

        wordToCount.print()

        ssc.start()
        ssc.awaitTermination()
    }

}

// nc -lp 9999
// 输入
