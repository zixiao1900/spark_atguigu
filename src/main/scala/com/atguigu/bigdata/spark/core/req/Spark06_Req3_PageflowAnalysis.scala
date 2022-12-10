package com.atguigu.bigdata.spark.core.req

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Req3_PageflowAnalysis {

  def main(args: Array[String]): Unit = {

    // TODO : 指定页面转化率 1-2  2-3  3-4
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)

    val actionRDD = sc.textFile("datas/user_visit_action.txt")
    // 转为样例类格式
    val actionDataRDD: RDD[UserVisitAction] = actionRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )
    actionDataRDD.cache()

    // TODO 对指定的页面连续跳转进行统计
    // 1-2,2-3,3-4,4-5,5-6,6-7
    val ids = List[Long](1, 2, 3, 4, 5, 6, 7)
    val okflowIds: List[(Long, Long)] = ids.init.zip(ids.tail)
    // todo 转为广播变量
    val ids_bc: Broadcast[List[Long]] = sc.broadcast(ids)
    val okflowIds_bc: Broadcast[List[(Long, Long)]] = sc.broadcast(okflowIds)

    // TODO 计算特定page分母
    val pageidToCountMap: Map[Long, Long] = actionDataRDD.filter(
      action => {
        // todo 广播变量 init 1-6不包括最后一个
        ids_bc.value.init.contains(action.page_id)
      }
    ).map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap

    // TODO 计算分子
    // 根据session进行分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)

    // 分组后，根据访问时间进行排序（升序）
    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)

        // 【1，2，3，4】
        // 【1，2】，【2，3】，【3，4】
        // 【1-2，2-3，3-4】
        // Sliding : 滑窗
        // 【1，2，3，4】
        // 【2，3，4】
        // zip : 拉链
        // todo 只需要pageID
        val flowIds: List[Long] = sortList.map(_.page_id)
        // [1,2,3] zip [2,3,4] -> [(1,2),(2,3),(3,4)]
        // init不包括最后一个  tail不包括第一个
        // todo (in, out)组合在后面做为过滤  不单独过滤 单独过滤会造成与真实情况不同的跳转组合
        val pageflowIds: List[(Long, Long)] = flowIds.init.zip(flowIds.tail)

        // todo 将不合法的页面跳转进行过滤
        // todo 这里过滤 过滤的直接是(page_front, page_after的组合 不是过滤单独一条)
        // todo 不会造成1,2,3 过滤2 留下1-3这种和真实情况不同的跳转组合
        val pageflowIdsFilterToOne: List[((Long, Long), Int)] = pageflowIds.filter(
          t => {
            // todo 广播变量
            okflowIds_bc.value.contains(t)
          }
        ).map(
          t => {
            (t, 1)
          }
        )
        pageflowIdsFilterToOne
      }
    )
    // ((1,2),1)
    val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(list => list)
    // ((1,2),1) => ((1,2),sum)
    val dataRDD: RDD[((Long, Long), Int)] = flatRDD.reduceByKey(_ + _)

    // TODO 计算单跳转换率
    // 分子除以分母  分子刚刚action触发
    val rateRDD = dataRDD.map {
      case ((pageid1, pageid2), sum_) => {
        val lon: Long = pageidToCountMap.getOrElse(pageid1, 0L)
        val rate = sum_.toDouble / lon

//        println(s"页面${pageid1}跳转到页面${pageid2}单跳转换率为:" + (sum_.toDouble / lon))
        (pageid1, pageid2, rate)
      }
    }
    rateRDD.foreach(println)
    actionRDD.unpersist()
    /*
    (2,3,0.019949423995504357)
    (4,5,0.018323153803442533)
    (1,2,0.01510989010989011)
    (3,4,0.016884531590413945)
    (5,6,0.014594442885209093)
    (6,7,0.0192040077929307)
     */


    sc.stop()
  }

  //用户访问动作表
  case class UserVisitAction(
                              date: String, //用户点击行为的日期
                              user_id: Long, //用户的ID
                              session_id: String, //Session的ID
                              page_id: Long, //某个页面的ID
                              action_time: String, //动作的时间点
                              search_keyword: String, //用户搜索的关键词
                              click_category_id: Long, //某一个商品品类的ID
                              click_product_id: Long, //某一个商品的ID
                              order_category_ids: String, //一次订单中所有品类的ID集合
                              order_product_ids: String, //一次订单中所有商品的ID集合
                              pay_category_ids: String, //一次支付中所有品类的ID集合
                              pay_product_ids: String, //一次支付中所有商品的ID集合
                              city_id: Long // 城市id
                            )
}
