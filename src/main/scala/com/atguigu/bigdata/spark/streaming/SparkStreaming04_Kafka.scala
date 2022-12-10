package com.atguigu.bigdata.spark.streaming

import java.util.Random

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_Kafka {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        // kafka配置
        val kafkaPara: Map[String, Object] = Map[String, Object](
            // 本机地址:集群地址
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "linux1:9092,linux2:9092,linux3:9092",
            // 消费者组topic
            ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )
        // 连接上了kafka
        // kafka传过来的key: String类型  value: String类型
        val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,  // 位置关系 框架自动匹配
            // todo 消费者策略 topic, params
            ConsumerStrategies.Subscribe[String, String](Set("atguiguNew"), kafkaPara)
        )
        // kafka的值
        kafkaDataDS.map(_.value()).print()


        ssc.start()
        ssc.awaitTermination()
    }

}
