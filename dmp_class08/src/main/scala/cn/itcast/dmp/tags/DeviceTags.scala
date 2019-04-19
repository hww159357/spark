package cn.itcast.dmp.tags

import cn.itcast.dmp.`trait`.Tags
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object DeviceTags extends Tags{
  /**
    * 给用户打标签的方法
    *
    * @param args 它表示参数的个数和类型不规定
    * @return Map[String,Double]---->key：标签名称，value:标签权重
    */
  override def makeTags(args: Any*): Map[String, Double] = {
    var map=Map[String, Double]()

    if(args.length>1){
      val row: Row = args(0).asInstanceOf[Row]
      val deviceMap: Map[String, String] = args(1).asInstanceOf[Map[String, String]]
      // println("deviceMap=="+deviceMap)

      //1、获取操作系统
       val client: Long = row.getAs[Long]("client")
       if(client !=null){
         //ios  android  wp  其他
         val os: String = deviceMap.getOrElse(client.toString,"D00010004")
         map+=(os ->1)
       }

      //2、获取网络类型
      val networkmannername: String = row.getAs[String]("networkmannername")
      if(StringUtils.isNotBlank(networkmannername)){
        val device: String = deviceMap.getOrElse(networkmannername,"D00020005")

        map+=(device ->1)
      }


      //3、获取网络运营商
      val ispname: String = row.getAs[String]("ispname")
      if(StringUtils.isNotBlank(ispname)){
        val isp: String = deviceMap.getOrElse(ispname,"D00030004")

        map+=(isp ->1)
      }
    }

    map
  }
}
