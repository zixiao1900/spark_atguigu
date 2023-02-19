分为4种 搜索数据  点击数据  下单数据  支付数据 每条数据一定是4种中的一种  _分割字段  字段中多值用,分割    下单和支付的CateID和 itemID不一定是一一对应
搜索:  time_userId_sessionID_pageID_eventTime_searchWords_-1_-1_null_null_null_null_cityID
点击:  time_userId_sessionID_pageID_eventTime_null_clickCateID_clickItemID_null_null_null_null_cityID
下单:  time_userId_sessionID_pageID_eventTime_null_-1_-1_orderCateID1,orderCateID2,orderCateID3_orderItemID1,orderItemID2_null_null_cityID
支付:  time_userId_sessionID_pageID_eventTime_null_-1_-1_null_null_payCateID1,payCateID2,payCateID3_payItemID1,payItemID2_cityID


需求1：
    根据每个品类的点击，下单，支付的量统计热门品类top10  按照点击，下单，支付的次数排序

需求2:
    需求1得到的10个 CateId, 找到其中每个CateId对应的最TOP10的sessionId

需求3:
    统计指定页面的单跳转化率