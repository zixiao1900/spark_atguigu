package com.atguigu.bigdata.spark.streaming

import java.text.SimpleDateFormat
import com.atguigu.bigdata.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming13_Req3 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.checkpoint("cp")

        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )

        val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("producer10"), kafkaPara)
        )
        val adClickData: DStream[AdClickData] = kafkaDataDS.map(
            kafkaData => {
                val data = kafkaData.value()
                val datas = data.split(" ")
                AdClickData(datas(0),datas(1),datas(2),datas(3),datas(4))
            }
        )

        // 最近一分钟，每10秒计算一次  12分01秒 -> 12分
        // 12:01 => 12:00
        // 12:11 => 12:10
        // 12:19 => 12:10
        // 12:25 => 12:20
        // 12:59 => 12:50

        // 55 => 50, 49 => 40, 32 => 30
        // 55 / 10 * 10 => 50
        // 49 / 10 * 10 => 40
        // 32 / 10 * 10 => 30

        // 这里涉及窗口的计算
        // todo 需求 流式统计最近1分钟的总点击数 每10秒做为一个key value是最近10秒统计的数量
        // todo reduceByKeyAndWindow统计部分虽然有重复 但是实际计算并没有重复 高性能
        val reduceDS = adClickData.map(
            data => {
                val ts = data.ts.toLong
                val newTS = ts / 10000 * 10000
                ( newTS, 1 )
            }
        ).reduceByKeyAndWindow(
            (x:Int,y:Int)=>{x+y},
            Seconds(60),
            Seconds(10)
        )

        reduceDS.print()
        /*
        (1669951970000,6)
        (1669951960000,9)
        (1669951940000,9)
        (1669951950000,11)
        (1669951910000,4)
        (1669951930000,14)
        (1669951920000,8)
         */


        ssc.start()
        ssc.awaitTermination()
    }
    // 广告点击数据
    case class AdClickData( ts:String, area:String, city:String, user:String, ad:String )

}
