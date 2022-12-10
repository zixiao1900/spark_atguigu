package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming08_Close {

    def main(args: Array[String]): Unit = {

        /*
           线程的关闭：
           val thread = new Thread()
           thread.start()

           thread.stop(); // 强制关闭

           todo 流任务 一般不会关闭采集器 但是当代码更新时 就需要关闭 要优雅的关闭
           todo 优雅的关闭就是不再接受新的数据 把已经接受的数据处理完毕再关闭
           todo 优雅的关闭 不能在main线程中 要再开一个线程
           todo 通过外部文件系统 例如redis mysql来控制内部程序关闭

         */

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val lines = ssc.socketTextStream("localhost", 9999)
        val wordToOne = lines.map((_,1))

        wordToOne.print()
        // todo 效果就是停止接受数据之后 会将已经接受到的数据处理完 再关闭

        ssc.start()

        // todo 如果想要关闭采集器，那么需要创建新的线程
        // 而且需要在第三方程序(redis, hdfs, mysql等)中增加关闭状态
        new Thread(
            new Runnable {
                override def run(): Unit = {
                    // 优雅地关闭
                    // 计算节点不在接收新的数据，而是将现有的数据处理完毕，然后关闭
                    // Mysql : Table(stopSpark) => Row => data
                    // Redis : Data（K-V）
                    // ZK    : /stopSpark
                    // HDFS  : /stopSpark

                    /*
                    // todo 真实情况的代码 但是不好测试 所以注释了
                    while ( true ) {
                        // todo if里面是写读取redis, mysql等数据 如果能读取到数据 就关闭
                        if (true) {
                            // 获取SparkStreaming状态
                            val state: StreamingContextState = ssc.getState()
                            if ( state == StreamingContextState.ACTIVE ) {
                                // todo 优雅的关闭
                                ssc.stop(true, true)
                            }
                        }
                        // 休眠一下再去取
                        Thread.sleep(5000)
                    }

                     */


                    // todo 测试代码 运行5秒之后判断 然后关闭
                    Thread.sleep(5000)
                    val state: StreamingContextState = ssc.getState()
                    if ( state == StreamingContextState.ACTIVE ) {
                        ssc.stop(true, true)
                    }
                    // 停止线程  在while True的情况下要用到
                    System.exit(0)
                }
            }
        ).start()

        ssc.awaitTermination() // block 阻塞main线程  后面就执行不到

        // 计算节点不再接收新的数据，而是将现有的数据处理完毕，然后关闭
        // todo 这里不能放在主线程中 放在哪里都不合适
//        ssc.stop(true, true)


    }

}
