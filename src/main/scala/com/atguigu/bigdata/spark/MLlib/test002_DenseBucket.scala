package com.atguigu.bigdata.spark.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.Bucketizer

object test002_DenseBucket {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)
    val dataFrame: DataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    dataFrame.show()
    /*
    +--------+
    |features|
    +--------+
    |  -999.9|
    |    -0.5|
    |    -0.3|
    |     0.0|
    |     0.2|
    |   999.9|
    +--------+
     */

    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    // Transform original data into its bucket index.
    val bucketedData = bucketizer.transform(dataFrame)

    println(s"Bucketizer output with ${bucketizer.getSplits.length-1} buckets")
    bucketedData.show()
    /*
    +--------+----------------+
    |features|bucketedFeatures|
    +--------+----------------+
    |  -999.9|             0.0|
    |    -0.5|             1.0|
    |    -0.3|             1.0|
    |     0.0|             2.0|
    |     0.2|             2.0|
    |   999.9|             3.0|
    +--------+----------------+
     */

    val splitsArray = Array(
      Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity),
      Array(Double.NegativeInfinity, -0.3, 0.0, 0.3, Double.PositiveInfinity))

    val data2 = Array(
      (-999.9, -999.9),
      (-0.5, -0.2),
      (-0.3, -0.1),
      (0.0, 0.0),
      (0.2, 0.4),
      (999.9, 999.9))
    val dataFrame2 = spark.createDataFrame(data2).toDF("features1", "features2")
    dataFrame2.show()
    /*
    +---------+---------+
    |features1|features2|
    +---------+---------+
    |   -999.9|   -999.9|
    |     -0.5|     -0.2|
    |     -0.3|     -0.1|
    |      0.0|      0.0|
    |      0.2|      0.4|
    |    999.9|    999.9|
    +---------+---------+
     */

    val bucketizer2 = new Bucketizer()
      .setInputCols(Array("features1", "features2"))
      .setOutputCols(Array("bucketedFeatures1", "bucketedFeatures2"))
      .setSplitsArray(splitsArray)

    // Transform original data into its bucket index.
    val bucketedData2 = bucketizer2.transform(dataFrame2)

    println(s"Bucketizer output with [" +
      s"${bucketizer2.getSplitsArray(0).length-1}, " +
      s"${bucketizer2.getSplitsArray(1).length-1}] buckets for each input column")
    bucketedData2.show()
    /*
    +---------+---------+-----------------+-----------------+
    |features1|features2|bucketedFeatures1|bucketedFeatures2|
    +---------+---------+-----------------+-----------------+
    |   -999.9|   -999.9|              0.0|              0.0|
    |     -0.5|     -0.2|              1.0|              1.0|
    |     -0.3|     -0.1|              1.0|              1.0|
    |      0.0|      0.0|              2.0|              2.0|
    |      0.2|      0.4|              2.0|              3.0|
    |    999.9|    999.9|              3.0|              3.0|
    +---------+---------+-----------------+-----------------+
     */
  }
}
