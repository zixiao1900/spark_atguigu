package com.atguigu.bigdata.spark.streaming

import java.util.{Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object SparkStreaming10_kafka_MockData {

    def main(args: Array[String]): Unit = {

        // todo 生成模拟数据
        // 格式 ：timestamp area city userid adid
        // 含义： 时间戳   区域  城市 用户 广告

        // Application => Kafka => SparkStreaming => Analysis
        val prop = new Properties()
        // 添加配置
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092") // 集群地址
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        // todo kafka生产者
        val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prop)

        while ( true ) {

            val tmpList: ListBuffer[String] = mockdata()
            tmpList.foreach(
                data => {
                    // todo 向Kafka中生成数据
                    val record = new ProducerRecord[String, String]("producer10", data)
                    // 生产者发送数据
                    producer.send(record)
                    println(data)
                }
            )
            // 每两秒发一次  一次随机生成1-49条数据(String)
            Thread.sleep(2000)
        }

    }
    def mockdata() = {
        val list: ListBuffer[String] = ListBuffer[String]()
        val areaList = ListBuffer[String]("华北", "华东", "华南")
        val cityList = ListBuffer[String]("北京", "上海", "深圳")
        // 每次随机生成1-49条数据
        for ( i <- 1 to new Random().nextInt(5) ) {
            // 随机生成are, city
            val area: String = areaList(new Random().nextInt(3))
            val city: String = cityList(new Random().nextInt(3))
            // 1-6
            val userid: String = (new Random().nextInt(6) + 1).toString
            val adid: String = (new Random().nextInt(6) + 1).toString


            list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
        }
        // timeStamp, area, city, userid, adid
        list
    }

}
// todo 使用方法
// 启动zookeeper cmd1: bin/windows/zookeeper-server-start.bat ./config/zookeeper.properties
// 启动kafka  cmd2: bin/windows/kafka-server-start.bat ./config/server.properties
// 启动kafka消费者 监听生产者产生的数据  cmd3: bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic producer10
// 然后启动程序 程序是kafka生产者  生产的数据会在消费者中监听到
