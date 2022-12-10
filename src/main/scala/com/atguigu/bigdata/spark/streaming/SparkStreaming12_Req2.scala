package com.atguigu.bigdata.spark.streaming

import java.text.SimpleDateFormat
import com.atguigu.bigdata.spark.streaming.SparkStreaming11_Req1_BlackList.AdClickData
import com.atguigu.bigdata.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.PreparedStatement

object SparkStreaming12_Req2 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // todo 实时统计每天各地地区各城市各广告的点击总流量，并将其存入MySQL
        //  1 微批次内数据进行按照天维度聚合统计
        //  2 结合MySQL数据跟当前批次数据跟新原有的数据
        /* MySQL中建表
        create table area_city_ad_count (
            dt VARCHAR(255),
            area VARCHAR(255),
            city VARCHAR(255),
            adid VARCHAR(255),
            count BIGINT,
        PRIMARY KEY (dt, area, city, adid)
        );
         */

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
        // 每天各地区各城市各广告做为key  wordCount
        val reduceDS: DStream[((String, String, String, String), Int)] = adClickData.map(
            data => {
                val sdf = new SimpleDateFormat("yyyy-MM-dd")
                val day = sdf.format(new java.util.Date( data.ts.toLong ))
                val area = data.area
                val city = data.city
                val ad = data.ad

                ( ( day, area, city, ad ), 1 )
            }
        ).reduceByKey(_+_)

        reduceDS.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    iter => {
                        // todo 一个分区只要连一次
                        val conn = JDBCUtil.getConnection
                        val pstat: PreparedStatement = conn.prepareStatement(
                            """
                              | insert into area_city_ad_count ( dt, area, city, adid, count )
                              | values ( ?, ?, ?, ?, ? )
                              | on DUPLICATE KEY
                              | UPDATE count = count + ?
                            """.stripMargin)
                        iter.foreach{
                            case ( ( day, area, city, ad ), sum ) => {
                                pstat.setString(1,day )
                                pstat.setString(2,area )
                                pstat.setString(3, city)
                                pstat.setString(4, ad)
                                pstat.setInt(5, sum)
                                pstat.setInt(6,sum )
                                // 真正执行sql  每个微批次的聚合结果 和累积的结果相加 更新到表中
                                pstat.executeUpdate()
                            }
                        }
                        pstat.close()
                        conn.close()
                    }
                )
            }
        )


        ssc.start()
        ssc.awaitTermination()
    }
    // 广告点击数据
    case class AdClickData( ts:String, area:String, city:String, user:String, ad:String )

}
