package com.atguigu.bigdata.spark.MLlib
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{DataFrame, SparkSession}

object test001_Encoding {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val df: DataFrame = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

    df.show()
    /*
    +---+--------+
    | id|category|
    +---+--------+
    |  0|       a|
    |  1|       b|
    |  2|       c|
    |  3|       a|
    |  4|       a|
    |  5|       c|
    +---+--------+
     */

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df).transform(df)
    indexed.show()
    /*
    +---+--------+-------------+
    | id|category|categoryIndex|
    +---+--------+-------------+
    |  0|       a|          0.0|
    |  1|       b|          2.0|
    |  2|       c|          1.0|
    |  3|       a|          0.0|
    |  4|       a|          0.0|
    |  5|       c|          1.0|
    +---+--------+-------------+


Process finished with exit code 0

     */
  }

}
