package com.atguigu.bigdata.spark.streaming

import java.sql.ResultSet
import java.text.SimpleDateFormat
import com.atguigu.bigdata.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object SparkStreaming11_Req1_BlackList {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // todo 需求1 实现实时的动态黑名单机制: 将每天对某个广告点击超过30次的用户拉黑 黑名单保存起来存文件
        // todo 流数据 如果用户在黑名单中 不做操作
        // todo 如果不再黑名单中 判断微批次聚合结果是否超过阈值 超过拉入拉入黑名单
        // todo 如果微批次没有超过阈值，更新每天的用户累积点击数量到hive 获取最新的累计数据 如果超过阈值就进入黑名单
        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )
        // 获取数据
        val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("producer10"), kafkaPara)
        )

        val adClickData: DStream[AdClickData] = kafkaDataDS.map(
            (kafkaData: ConsumerRecord[String, String]) => {
                val data: String = kafkaData.value()
                val datas: Array[String] = data.split(" ")
                AdClickData(datas(0),datas(1),datas(2),datas(3),datas(4))
            }
        )

        val ds: DStream[((String, String, String), Int)] = adClickData.transform(
            rdd => {


                // TODO driver端  rdd算子外写连接操作
                //  通过JDBC周期性获取黑名单数据  这里依旧是driver端 这里获得的数据库连接 不能放到rdd.map里面用 有序列化问题
                //  每来一个微批的数据 都需要判断用户是否在黑名单中 需要从数据库中取一次 黑名单每个微批都有可能更新
                val blackList = ListBuffer[String]()

                val conn = JDBCUtil.getConnection
                val pstat = conn.prepareStatement("select userid from black_list")
                // 真正执行查询
                val rs: ResultSet = pstat.executeQuery()
                while ( rs.next() ) {
                    blackList.append(rs.getString(1))
                }

                rs.close()
                pstat.close()
                conn.close()

                // TODO 判断点击用户是否在黑名单中
                val filterRDD: RDD[AdClickData] = rdd.filter(
                    data => {
                        !blackList.contains(data.user)
                    }
                )

                // TODO 如果用户不在黑名单中，那么进行统计数量（每个采集周期3s）
                // 要求是每天 每个广告的个数  所以要有day 和 ad两个字段
                val countRDD: RDD[((String, String, String), Int)] = filterRDD.map(
                    data => {
                        // 13timestamp -> Day
                        val sdf = new SimpleDateFormat("yyyy-MM-dd")
                        val day = sdf.format(new java.util.Date( data.ts.toLong ))

                        val user = data.user
                        val ad = data.ad

                        (( day, user, ad ), 1) // (word, count)
                    }
                ).reduceByKey(_+_)
                countRDD
            }
        )

        ds.foreachRDD(
            rdd => {
                rdd.foreach{
                    case ( ( day, user, ad ), count ) => {
                        // 采集周期内统计结果
                        println(s"${day} ${user} ${ad} ${count}")
                        if ( count >= 30 ) {
                            // TODO 如果微批次统计数量超过点击阈值(30)，那么将用户拉入到黑名单
                            val conn = JDBCUtil.getConnection
                            // 可能用户点了31次A广告 插入一次黑名单 还点了32次B广告 那么这个操作就重复了
                            // 所以用了 on DUPLICATE KEY update
                            val pstat = conn.prepareStatement(
                                """
                                  |insert into black_list (userid) values (?)
                                  |on DUPLICATE KEY
                                  |UPDATE userid = ?
                                """.stripMargin)
                            pstat.setString(1, user)
                            pstat.setString(2, user)
                            pstat.executeUpdate()
                            pstat.close()
                            conn.close()
                        }
                        else {
                            // TODO 如果微批次没有超过阈值，那么需要将当天的广告点击数量进行更新。
                            val conn = JDBCUtil.getConnection
                            val pstat = conn.prepareStatement(
                                """
                                  | select
                                  |     *
                                  | from user_ad_count
                                  | where dt = ? and userid = ? and adid = ?
                                """.stripMargin)

                            pstat.setString(1, day)
                            pstat.setString(2, user)
                            pstat.setString(3, ad)
                            val rs = pstat.executeQuery()
                            // 查询统计表数据
                            if ( rs.next() ) {
                                // todo 1 如果存在数据，那么更新
                                val pstat1 = conn.prepareStatement(
                                    """
                                      | update user_ad_count
                                      | set count = count + ?
                                      | where dt = ? and userid = ? and adid = ?
                                    """.stripMargin)
                                pstat1.setInt(1, count)
                                pstat1.setString(2, day)
                                pstat1.setString(3, user)
                                pstat1.setString(4, ad)
                                pstat1.executeUpdate()
                                pstat1.close()
                                // TODO 1.1 判断更新后的点击数据是否超过阈值，如果超过，那么将用户拉入到黑名单。
                                val pstat2 = conn.prepareStatement(
                                    """
                                      |select
                                      |    *
                                      |from user_ad_count
                                      |where dt = ? and userid = ? and adid = ? and count >= 30
                                    """.stripMargin)
                                pstat2.setString(1, day)
                                pstat2.setString(2, user)
                                pstat2.setString(3, ad)
                                val rs2 = pstat2.executeQuery()
                                if ( rs2.next() ) {
                                    val pstat3 = conn.prepareStatement(
                                        """
                                          |insert into black_list (userid) values (?)
                                          |on DUPLICATE KEY
                                          |UPDATE userid = ?
                                        """.stripMargin)
                                    pstat3.setString(1, user)
                                    pstat3.setString(2, user)
                                    pstat3.executeUpdate()
                                    pstat3.close()
                                }

                                rs2.close()
                                pstat2.close()
                            }
                            else {
                                // todo 2 如果不存在数据，那么新增
                                val pstat1 = conn.prepareStatement(
                                    """
                                      | insert into user_ad_count ( dt, userid, adid, count ) values ( ?, ?, ?, ? )
                                    """.stripMargin)

                                pstat1.setString(1, day)
                                pstat1.setString(2, user)
                                pstat1.setString(3, ad)
                                pstat1.setInt(4, count)
                                pstat1.executeUpdate()
                                pstat1.close()
                            }

                            rs.close()
                            pstat.close()
                            conn.close()
                        }
                    }
                }
            }
        )

        ssc.start()
        ssc.awaitTermination()
    }
    // 广告点击数据
    case class AdClickData( ts:String, area:String, city:String, user:String, ad:String )

}
