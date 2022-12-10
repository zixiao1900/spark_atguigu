package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_map_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - map
        val rdd: RDD[String] = sc.textFile("datas/apache.log")

        // 长的字符串
        // 短的字符串
        val mapRDD: RDD[String] = rdd.map(
            line => {
                val datas = line.split(" ")
                datas(6)
            }
        )
        mapRDD.collect().take(5).foreach(println)
        /*  根据空格分割 遇到idx为6的列
        /presentations/logstash-monitorama-2013/images/kibana-search.png
        /presentations/logstash-monitorama-2013/images/kibana-dashboard3.png
        /presentations/logstash-monitorama-2013/plugin/highlight/highlight.js
        /presentations/logstash-monitorama-2013/plugin/zoom-js/zoom.js
        /presentations/logstash-monitorama-2013/plugin/notes/notes.js
         */

        sc.stop()

    }
}
