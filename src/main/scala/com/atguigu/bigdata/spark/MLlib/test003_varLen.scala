package com.atguigu.bigdata.spark.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object test003_varLen {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val data = Seq((1, "a:1#b:2#c:3"), (2, "aa:1#bb:2#cc:3"))
    val df: DataFrame = spark.createDataFrame(data).toDF("id", "varLen_feat")
    df.show()
    df.printSchema()
    /*
    +---+--------------+
    | id|   varLen_feat|
    +---+--------------+
    |  1|   a:1#b:2#c:3|
    |  2|aa:1#bb:2#cc:3|
    +---+--------------+
     */

    // "a:1#b:2#c:3" 只要a
    val first_df = df.rdd.map{
      case Row(id:Int, varLen:String) => {
        val outSplit = "#"
        val innerSplit = ":"
        val firstValue: String = varLen.split(outSplit).head.split(innerSplit).head
        (id, firstValue)
      }
    }.toDF()
    first_df.show()
    /*
    +---+---+
    | _1| _2|
    +---+---+
    |  1|  a|
    |  2| aa|
    +---+---+
     */


  }
}
