package com.atguigu.bigdata.spark.streaming

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object loadKafkaJson {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("loadKafkaJson")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val bootstrapServers = "localhost:9092"
    val topicName = "topicCar1"
    val groupId = "atguigu1"
    val kafkaParas: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers, // 集群地址
      ConsumerConfig.GROUP_ID_CONFIG -> groupId, // group 似乎不重要
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topicName), kafkaParas)
    )

    val schema = StructType(
      List(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("age", IntegerType)
      )
    )

    // todo transform
    val lineDS: DStream[String] = kafkaDataDS.map(_.value())
    lineDS.print()
    /*
        "{\"data\": [{\"id\":100, \"name\": \"Tom\", \"age\": 15},{\"id\":101, \"name\": \"Alice\", \"age\": 14},{\"id\":102, \"name\": \"Peter\", \"age\": 13}]}"
     */

    lineDS.window(
      Minutes(1), Minutes(1)
    ).foreachRDD(rdd => {
      val rowRDD: RDD[Row] = rdd.map(str => JSON.parseObject(str).getJSONArray("data"))
        .map(jsonArray => {
          val result = ArrayBuffer[JSONObject]()
          for (i <- 0 until jsonArray.size()) {
            result.append(jsonArray.getJSONObject(i))
          }
          result
        }).flatMap(x => x)
        .map(x => Row(
          x.getInteger("id"),
          x.getString("name"),
          x.getInteger("age")
        ))
      val windowDF = spark.createDataFrame(rowRDD, schema)

      val timeArr: Array[String] = new SimpleDateFormat("yyyyMMdd HH mm").format(new Date()).split(" ")
      val date: String = timeArr(0)
      val hour: String = timeArr(1)
      val minute: String = timeArr(2)

      windowDF.write.format("parquet").mode(SaveMode.Overwrite)
        .save(s"outputs/car_raw_table/${date}/${hour}/${minute}")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
