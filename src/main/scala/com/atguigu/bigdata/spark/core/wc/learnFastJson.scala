package com.atguigu.bigdata.spark.core.wc

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.io.{BufferedSource, Source}

object learnFastJson {
  def main(args: Array[String]): Unit = {
    val inputStr: String = Source.fromFile("datas/learn.json").mkString
    println("inputStr:------")
    println(inputStr)
    println(inputStr.getClass)
    
    // TODO STRING -> JSONObject
    val json: JSONObject = JSON.parseObject(inputStr)
    println("parse json:-----")
    println(json)
    println(json.getClass)

    // TODO JSONObject.getString    JSONObject.getInteger
    val et: String = json.getString("et")
    val vtm: Integer = json.getInteger("vtm")
    println(et)
    println(et.getClass)
    println(vtm)
    println(vtm.getClass)

    // TODO 内部嵌套jsonObject  JSONObject.getJSONObject
    val body: JSONObject = json.getJSONObject("body")
    val client: String = body.getString("client")
    val clientType: String = body.getString("client_type")
    val room: String = body.getString("room")
    val gid: String = body.getString("gid")
    val type_ : String = body.getString("type")
    val roomId: String = body.getString("roomid")
    println(client)
    println(clientType)
    println(room)
    println(gid)
    println(type_)
    println(roomId)

    // todo 内部是jsonArray  JSONObject.getJSONArray
    val time: JSONArray = json.getJSONArray("time")
    println(time)
    println(time.getClass)

    println(time.get(0))

    // todo jsonArray内部又是jsonObject JSONArray.getJSONObject(idx)
    println(time.getJSONObject(0).getString("arrayKey"))
    println(time.getJSONObject(1).getString("key2"))
  }
}
