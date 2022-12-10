package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_reduceByKeyAndWindow {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")

        val lines = ssc.socketTextStream("localhost", 9999)
        val wordToOne: DStream[(String, Int)] = lines.map((_,1))

        // reduceByKeyAndWindow : 当窗口范围比较大，但是滑动幅度比较小，那么可以采用增加数据和删除数据的方式
        // todo 采集周期之间有重复性的统计 但是无需重复计算，提升性能。
        // todo 统计最近3个采集周期的wordCount 每个采集周期统计一次
        val windowDS: DStream[(String, Int)] =
            wordToOne.reduceByKeyAndWindow(
                (x:Int, y:Int) => { x + y}, // 增加的数据
                (x:Int, y:Int) => {x - y},  // todo 减少的数据 用这个可以减少计算 但是要设置checkpoint
                Seconds(9), Seconds(3)) // 如果窗口和步长设置相同 那就是正常的滚动操作

        windowDS.print()

        ssc.start()
        ssc.awaitTermination()
    }

}
