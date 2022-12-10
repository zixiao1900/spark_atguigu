package com.atguigu.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming11_Req1 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val spark = SparkSession.builder()
          .enableHiveSupport() // todo enableHiveSupport一定要写
          .config(sparkConf).getOrCreate()
        val sc = spark.sparkContext
        val ssc = new StreamingContext(sc, Seconds(3))
        import spark.implicits._
        System.setProperty("user.name", "hdfs")

        spark.sql(
            """
              |CREATE TABLE IF NOT EXISTS `black_list4`(
              |  `userid` String)
              |row format delimited fields terminated by '\t'
            """.stripMargin)

        spark.sql(s"select * from black_list4").show(100, false)

        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",  // 集群地址
            ConsumerConfig.GROUP_ID_CONFIG -> "atguigu1", // group 似乎不重要
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )
        // kafka消费者
        val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("producer10"), kafkaPara)
        )
        val ds: DStream[String] = kafkaDataDS.map(_.value())
        ds.foreachRDD(
            (rdd: RDD[String]) => {
                val strings: Array[String] = rdd.collect()
                strings.foreach(
                    s => {
//                        spark.sql(
//                            s"""
//                               |insert into table black_list3 values ('${s}')
//                               |""".stripMargin)
                        spark.sql(s"select * from black_list4").show()
                    }
                )
            }
        )

//        ds.foreachRDD(
//            rdd => {
//                spark.sql(s"select * from black_list3").show()
//                rdd.foreachPartition(
//                    iter => {
//
//                        iter.foreach(
//                            s => {
//                                println(s)
//
//                            }
//
//                        )
//                    }
//                )
//            }
//        )

        ssc.start()
        ssc.awaitTermination()
    }

}
