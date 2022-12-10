package com.atguigu.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming05_State {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        // todo 使用有状态操作时，需要设定检查点路径
        ssc.checkpoint("cp")

        // todo 无状态数据操作，只对当前的采集周期内的数据进行处理
        // 在某些场合下，需要保留数据统计结果（状态），实现数据的汇总
        val datas = ssc.socketTextStream("localhost", 9999)

        val wordToOne = datas.map((_,1))

//        val wordToCount = wordToOne.reduceByKey(_+_)

//        wordToCount.print()

        // todo updateStateByKey：根据key对数据的状态进行更新  当前例子是演示从数据流开始存在 到当前时间 累计统计的wordCount
        // 传递的参数中含有两个值
        // 第一个值表示相同的key的value数据
        // 第二个值表示缓存区相同key的value数据
        val state = wordToOne.updateStateByKey(
            //todo seq: 当前微批次 相同Key的value的集合
            //todo buff: 相同key缓冲区value数据
            ( seq:Seq[Int], buff:Option[Int] ) => {
                val newCount = buff.getOrElse(0) + seq.sum
                Option(newCount)
            }
        )
        // todo 这里打印的都是累积的wordCount
        state.print()


        ssc.start()
        ssc.awaitTermination()
    }

}
