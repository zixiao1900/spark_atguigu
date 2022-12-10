package com.atguigu.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_groupBy_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - groupBy
        // todo 从服务器日志数据apache.log中获取每个小时访问量  找到访问量在24小时中的分布
        val rdd = sc.textFile("datas/apache.log")

        val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
            line => {
                val datas = line.split(" ")
                val time = datas(3)
                val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                val date: Date = sdf.parse(time)  // Wed May 20 20:05:48 CST 2015
//                val hour: Int = date.getHours
                val sdf1 = new SimpleDateFormat("HH")
                val hour: String = sdf1.format(date) //20
                (hour, 1)
            }
        ).groupBy(_._1)

        timeRDD.map{
            case ( hour, iter ) => {
                (hour, iter.size)
            }
        }.sortBy(tup=>tup._1, ascending = true) // 排序之后输出
          .collect().foreach(println)

        /*
        (4,355)
        (16,473)
        (22,346)
        (14,498)
        (0,361)
        (6,366)
        (18,478)
        (12,462)
        (20,486)
        (8,345)
        (10,443)
        (2,365)
        (13,475)
        (19,493)
        (15,496)
        (21,453)
        (11,459)
        (23,356)
        (1,360)
        (17,484)
        (3,354)
        (7,357)
        (9,364)
        (5,371)
         */


        sc.stop()

    }
}
