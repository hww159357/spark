package cn.itcast.dmp.trade

import cn.itcast.dmp.tools.{GlobalConfigUtils, ParseJson}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod

object ExtraTrade {
  private val key: String = GlobalConfigUtils.getKey

  //解析经纬度  获取得到商圈信息
  def parseLocation(location:String):String={
    //https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=1c9d3317451726a6d345b3e00d0c46de
    val url="https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key="+key

    val httpClient = new HttpClient()
    val method = new GetMethod(url)

    var tmp=""

    //发送url请求
    val status: Int = httpClient.executeMethod(method)
      //针对于响应码做一个判断
    if(status==200){
       val response: String = method.getResponseBodyAsString
       //解析 json串
      tmp=ParseJson.parseJson(response)

    }
    tmp
  }
}
