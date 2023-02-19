package com.atguigu.bigdata.spark.core.wc

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.io.Source

object learnFastJson2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("learnJson2")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val inputStr: String = Source.fromFile("datas/car.json").mkString
    println("inputStr:------")
    println(inputStr)
    println(inputStr.getClass)

    val data: JSONArray = JSON.parseObject(inputStr).getJSONArray("data")
    println(data)


  }
}
