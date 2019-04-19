package cn.itcast.dmp.tools

import cn.itcast.dmp.bean.BusinessArea
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.lang.StringUtils

object ParseJson {
  /**
     <regeocode>
        <addressComponent>
              <businessAreas type="list">
                    <businessArea>
                        <location>116.29522008325625,39.99426090286774</location>
                        <name>颐和园</name>
                        <id>110108</id>
                    </businessArea>
                      <businessArea>
                          <location>116.31060892521111,39.99231773703259</location>
                          <name>北京大学</name>
                          <id>110108</id>
                      </businessArea>
                      <businessArea>
                          <location>116.32013920092481,39.97507461118122</location>
                          <name>中关村</name>
                          <id>110108</id>
                      </businessArea>
             </businessAreas>
       </addressComponent>
    </regeocode>
    */
  def parseJson(jsonStr:String):String={
         val jSONObject: JSONObject = JSON.parseObject(jsonStr)
         val regeocode: JSONObject = jSONObject.get("regeocode").asInstanceOf[JSONObject]

    //定义一个标识
       var flag=""
         if(regeocode !=null) {
           val addressComponent: JSONObject = regeocode.get("addressComponent").asInstanceOf[JSONObject]
           val jSONArray: JSONArray = addressComponent.getJSONArray("businessAreas")

          import scala.collection.JavaConversions._
         val businessAreas: List[BusinessArea] = JSON.parseArray(jSONArray.toJSONString,classOf[BusinessArea]).toList



        val sb = new StringBuffer()
        //拼接所有商圈信息
       if(businessAreas.size>0){    //颐和园:北京大学:中关村:
         businessAreas.filter(x=> x!=null ).foreach(x=>sb.append(x.name+":"))

         val value: String = sb.toString
//          println("商圈信息："+value)
         if(StringUtils.isNotBlank(value)){
           //颐和园:北京大学:中关村:   ----->  颐和园:北京大学:中关村
           flag=value.substring(0,value.length-1)

         }
       }
         }
    flag
  }

}
