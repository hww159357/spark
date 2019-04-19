package cn.itcast.dmp.tags

import java.util

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row


object UserUnqueTags{
  /**
    * 给用户打标签的方法
    *
    * @param args 它表示参数的个数和类型不规定
    * @return Map[String,Double]---->key：标签名称，value:标签权重
    */
  def makeTags(args: Any*): util.LinkedList[String] = {
   val list = new util.LinkedList[String]()

    if(args.length >0){
      val row: Row = args(0).asInstanceOf[Row]

      //手机的串号imei
       val imei: String = row.getAs[String]("imei")
       if(StringUtils.isNotBlank(imei)){
        list.add("IMEI:"+imei)
      }
      val imeimd5: String = row.getAs[String]("imeimd5")
      if(StringUtils.isNotBlank(imeimd5)){
        list.add("IMEIMD5:"+imeimd5)
      }
       val imeisha1: String = row.getAs[String]("imeisha1")
      if(StringUtils.isNotBlank(imeisha1)){
        list.add("IMEISHA1:"+imeisha1)
      }

      //手机MAC码
      val mac: String = row.getAs[String]("mac")
      if(StringUtils.isNotBlank(mac)){
        list.add("MAC:"+mac)
      }
      val macmd5: String = row.getAs[String]("macmd5")
      if(StringUtils.isNotBlank(macmd5)){
        list.add("MACMD5:"+macmd5)
      }
      val macsha1: String = row.getAs[String]("macsha1")
      if(StringUtils.isNotBlank(macsha1)){
        list.add("MACSHA1:"+macsha1)
      }

      //手机APP的广告码
      val idfa: String = row.getAs[String]("idfa")
      if(StringUtils.isNotBlank(idfa)){
        list.add("IDFA:"+idfa)
      }
      val idfamd5: String = row.getAs[String]("idfamd5")
      if(StringUtils.isNotBlank(idfamd5)){
        list.add("IDFAMD5:"+idfamd5)
      }
      val idfasha1: String = row.getAs[String]("idfasha1")
      if(StringUtils.isNotBlank(idfasha1)){
        list.add("IDFASHA1:"+idfasha1)
      }

      //苹果设备的识别码
      val openudid: String = row.getAs[String]("openudid")
      if(StringUtils.isNotBlank(openudid)){
        list.add("OPENUDID:"+openudid)
      }
      val openudidmd5: String = row.getAs[String]("openudidmd5")
      if(StringUtils.isNotBlank(openudidmd5)){
        list.add("OPENUDIDMD5:"+openudidmd5)
      }
      val openudidsha1: String = row.getAs[String]("openudidsha1")
      if(StringUtils.isNotBlank(openudidsha1)){
        list.add("OPENUDIDSHA1:"+openudidsha1)
      }

      //安卓设备的识别码
      val androidid: String = row.getAs[String]("androidid")
      if(StringUtils.isNotBlank(androidid)){
        list.add("ANDROIDID:"+androidid)
      }
      val androididmd5: String = row.getAs[String]("androididmd5")
      if(StringUtils.isNotBlank(androididmd5)){
        list.add("ANDROIDIDDMD5:"+androididmd5)
      }
      val androididsha1: String = row.getAs[String]("androididsha1")
      if(StringUtils.isNotBlank(androididsha1)){
        list.add("ANDROIDIDSHA1:"+androididsha1)
      }

    }
    list
  }
}
